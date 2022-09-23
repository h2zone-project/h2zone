// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License 
//  (found in the LICENSE.Apache file in the root directory).  

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <sys/time.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <string>
#include <sstream>
#include <fstream>
#include <utility>
#include <vector>
#include <map>

#include "rocksdb/env.h"

#include "fs_zenfs.h"
#include "io_zenfs.h"
#include "cache_manager.h"
#include "util.h"

namespace ROCKSDB_NAMESPACE {

// use the global mapped offset instead of the offset in each device
// Optimize from O(n) to O(logn)
Zone *HybridBlockDevice::GetIOZone(uint64_t foffset) {
  for (int i = 0; i < (int)zbds_.size(); i++) {
    ZonedBlockDevice* zbd = zbds_[i];
    uint64_t zone_size = zbd->zone_sz();

    if (zbd->fstart() > foffset || zbd->fend() < foffset) {
      continue;
    } 

    uint64_t fstart_zone = 
      (foffset - zbd->fstart()) / zone_size * zone_size + zbd->fstart(); 

    auto iter = fstart_2_io_zone_[i].find(fstart_zone);
    if (iter == fstart_2_io_zone_[i].end()) {
      return nullptr;
    }

    return iter->second;
  } 

//  for (const auto z : io_zones_) {
//    if (z->fstart() <= foffset && 
//        foffset < (z->fstart() + z->max_capacity())) {
//      return z;
//    }
//  }
  return nullptr;
}

HybridBlockDevice::HybridBlockDevice(std::vector<std::string> bdevnames,
                                   std::shared_ptr<Logger> logger)
    : logger_(logger) {

  fused_filename_ = "";
  bool first = true;
  zops_.Read();

  for (auto& bdevname : bdevnames) {
    fused_filename_ += (first) ? bdevname : "_" + bdevname;
    first = false;
    filenames_.push_back("/dev/" + bdevname);

    ZonedBlockDevice* zbd = new ZonedBlockDevice(bdevname, logger);
    zbds_.push_back(zbd);
  }

  Info(logger_, "New Hybrid Zoned Block Device: %s\n", fused_filename_.c_str());

  std::string str = fused_filename();
  level_m_ = zops_.level_m;

  if (zops_.enable_cache_manager) {
    cache_ = new CacheManager(this, zops_);
  }

  stat_ = new ZenStat(zops_);
  stop_ = false;
};

IOStatus HybridBlockDevice::CheckScheduler() {
  for (auto s : filenames_) {
    std::ostringstream path;
    std::fstream f;

    s.erase(0, 5); // Remove "/dev/" from /dev/nvmeXnY
    path << "/sys/block/" << s << "/queue/scheduler";
    f.open(path.str(), std::fstream::in);
    if (!f.is_open()) {
      return IOStatus::InvalidArgument("Failed to open " + path.str());
    }

    std::string buf;
    getline(f, buf);
    if (buf.find("[mq-deadline]") == std::string::npos) { // Only support mq-deadline
      f.close();
      return IOStatus::InvalidArgument("(Hybrid) Current ZBD scheduler " + s
          + " is not mq-deadline, set it to mq-deadline.");
    }

    f.close();
  }

  return IOStatus::OK();
}

IOStatus HybridBlockDevice::Open(bool readonly) {
  Status s;
  uint64_t global_offset = 0;

  max_nr_active_io_zones_ = 0;
  max_nr_open_io_zones_ = 0;

  nr_zones_ = 0;
  global_offsets_.clear();
  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (auto& it : zbds_) {
    // We ignore meta_zones_ and io_zones_ in ZonedBlockDevice
    IOStatus ios = it->Open(readonly); 

    nr_zones_ += it->nr_zones_;
    if (ios != IOStatus::OK()) {
      return ios;
    }

    /* We need one open zone for meta data writes, the rest can be used for files */
    max_nr_active_io_zones_ += it->max_nr_active_io_zones_;
    max_nr_open_io_zones_ += it->max_nr_open_io_zones_;
    active_io_zones_ += it->active_io_zones_;
    open_io_zones_ += it->open_io_zones_;

    global_offsets_.push_back(global_offset);
    it->SetFstart(global_offset);

    if (!it->zoned_) {
      it->SetFend(global_offset + (uint64_t)it->nr_zones_ * it->zone_cap_ - 1);
      global_offset += (uint64_t)it->nr_zones_ * it->zone_cap_;
    } else {
      it->SetFend(global_offset + (uint64_t)it->nr_zones_ * it->zone_sz_ - 1);
      global_offset += (uint64_t)it->nr_zones_ * it->zone_sz_;
    }

    Info(logger_, "Zone block device %s nr zones: %u max active: %u max open: %u \n",
         it->filename_.c_str(), 
         it->nr_zones_, 
         it->max_nr_active_io_zones_, 
         it->max_nr_open_io_zones_);
  }

  // Use only one active metazone
  max_nr_active_io_zones_ --;
  max_nr_open_io_zones_ --;

  Info(logger_, "Zone block device nr zones(%u) max active(%u) max open(%u)\n",
       nr_zones_, max_nr_active_io_zones_, max_nr_open_io_zones_);

  meta_zones_.clear();
  io_zones_.clear();

  for (int j = 0; j < (int)zbds_.size(); j++) {
    ZonedBlockDevice* zbd = zbds_[j];
    bool has_meta_zone = false;
    bool has_active_meta_zone = false;
    global_offset = global_offsets_[j];

    fstart_2_io_zone_.push_back({});

    for (auto& zone : zbd->meta_zones_) {  // Select the meta zones
      zone->SetFstart(global_offset + zone->start());
      
      if (meta_zones_.size() < ZENFS_META_ZONES) { 
        meta_zones_.push_back(zone);
        has_meta_zone = true;

        if (!zone->IsEmpty() && !zone->IsFull()) {
          has_active_meta_zone = true;
        } 
      } else {
        io_zones_.push_back(zone);
        fstart_2_io_zone_[j][zone->fstart()] = zone;
      }
    }
//    zbd->meta_zones_.clear();

    if (has_meta_zone) {
      zbd->max_nr_active_io_zones_ --;
      zbd->max_nr_open_io_zones_ --;

      if (has_active_meta_zone) {
        PutActiveIOZoneToken();
        zbd->PutActiveIOZoneToken();
      }
    }

    for (auto& zone : zbd->io_zones_) {
      zone->SetFstart(global_offset + zone->start());

      // From v1.0.9, leverage WAL zones as cache zones 
      io_zones_.push_back(zone);
      fstart_2_io_zone_[j][zone->fstart()] = zone;

      if (!zone->rotational() && 
          (zops_.enable_data_placement || zops_.enable_spandb_data_placement) && 
          num_wal_zones_ < (int)(zops_.max_wal_size / zone->max_capacity())) {
        zone->SetForWal();
        num_wal_zones_++;
      }
    }
//    zbd->io_zones_.clear();
  }

  // Get SSD and HDD total space
  {
    ssd_total_space_ = 0;
    hdd_total_space_ = 0;
    for (const auto z : io_zones_) {
      if (!z->rotational()) {
        ssd_total_space_ += z->max_capacity();
      } else {
        hdd_total_space_ += z->max_capacity();
      }
    }
  }

  start_time_ = time(NULL);
  start_time_double = gettime();

  LogZoneUsage();

  return IOStatus::OK();
}

void HybridBlockDevice::LogZoneUsage() {
  char msg[200];
  int ptr, first;
  bool is_ssd = false;

  io_zones_mtx_.lock();

  std::map<int, int> ssd_lvl_2_zones;
  std::map<int, int> hdd_lvl_2_zones;

  for (int i = 0; i < (int)io_zones_.size(); i++) {
    const auto z = io_zones_[i];

    int64_t used = z->used_capacity_;
    is_ssd = !z->rotational();
    msg[0] = '\0'; 
    ptr = 0, first = 1;

    std::map<int, int> levels = z->levels_;
//    std::map<uint64_t, uint64_t> used_sizes = z->used_sizes_;

    for (auto& it : levels) {
      if (it.second > 0) {
        sprintf(msg + ptr, "%sL%d_%d", 
            first ? "" : "-", 
            it.first, it.second);
        ptr = strlen(msg);
        first = 0;
      }
    }

//    for (auto& it : used_sizes) {
//      sprintf(msg + ptr, ";%ld_%.3lf",
//          it.first, (double)it.second / z->max_capacity()); 
//      ptr = strlen(msg);
//    }

    // Patch - Fix the level of zone
    if (levels.size() == 1) {
      for (auto& it : levels) {
        z->SetLevel(it.first);
      }
    }

    int rt = z->rand_read_times_;

    if (used > 0) {
      Debug(logger_, "Z %s %ld used "
          "%.2lf MB l %d t %d util %.2lf %% [%s] "
          "%s %s %3.lf %% rt %d lf %.2lf cb %.2lf MB\n",
            is_ssd ? "ssd" : "hdd",
            z->zone_nr(), (double)used / MB, 
            (int)z->level(), (int)z->file_type(),
            z->util() * 100.0, 
            msg, 
            z->IsFull() ? "f" : "nf", 
            z->open_for_write_ ? "o" : "c", 
            z->wp_pct(), 
            rt, 
            z->lifespan(), 
            z->CompactedBytes() / 1024.0 / 1024.0);

      if (is_ssd) {
	ssd_lvl_2_zones[z->level()]++;
      } else {
	hdd_lvl_2_zones[z->level()]++;
      }
    } else if (is_ssd) {
      // Empty zone, should print before reset, or calculated as no level zones

      Debug(logger_, "Z %s  [empty]     "
          "               lvl %d type %d (%s) %s %s [%s] %.3lf %%\n",
            is_ssd ? "ssd" : "hdd",
            (int)z->level(), (int)z->file_type(),
            z->open_for_write_ ? "o" : "c", 
            z->for_wal() ? "w" : "", 
            z->for_cache() ? "c" : "", 
            msg, z->wp_pct());

      ssd_lvl_2_zones[999]++;
    }
  }

  stat_->LogLevelBytes(logger_.get());

  for (auto& it : ssd_lvl_2_zones) {
    Debug(logger_, "ssd zones level %d %d\n", (int)it.first, it.second);
  }

  for (auto& it : hdd_lvl_2_zones) {
    Debug(logger_, "hdd zones level %d %d\n", (int)it.first, it.second);
  }

  io_zones_mtx_.unlock();
}

void HybridBlockDevice::FlushZoneWps() {
  FILE* fp = fopen("/tmp/conv_wp", "w");

  if (fp == NULL) {
    return;
  }

  for (int i = 0; i < (int)meta_zones_.size(); i++) {
    Zone* z = meta_zones_[i];
    if (z->zoned()) continue;
    fprintf(fp, "%ld %ld\n", z->start(), z->wp());
  }

  for (int i = 0; i < (int)io_zones_.size(); i++) {
    Zone* z = io_zones_[i];
    if (z->zoned()) continue;
    fprintf(fp, "%ld %ld\n", z->start(), z->wp());
  }

  fclose(fp);
}

HybridBlockDevice::~HybridBlockDevice() {
  FlushZoneWps();

  for (const auto z : meta_zones_) {
    delete z;
  }

  for (const auto z : io_zones_) {
    delete z;
  }

  for (auto& zbd : zbds_) {
    delete zbd;
  }

  PrintStat();

  if (cache_) {
    delete cache_;
  }
}

// In AllocateZone()
Zone* HybridBlockDevice::TryResetAndFinish(ZonedBlockDevice* zbd_p) {
  Zone* finish_victim = nullptr;
  IOStatus s;
  for (int i = 0; i < (int)io_zones_.size(); i++) {
    const auto z = io_zones_[i];

    ZonedBlockDevice* zbd = z->zbd();
    if (zbd_p && zbd != zbd_p) {
      continue;
    }

    if (z->open_for_write_ || z->IsEmpty() || (z->IsFull() && z->IsUsed()))
      continue;

    if (z->for_cache()) 
      continue;

    // An not empty but not used zone, reset
    if (!z->IsUsed()) {
      if (!z->IsFull()) {
        PutActiveIOZoneToken();
        zbd->PutActiveIOZoneToken(); 
      }

      s = z->Reset();
      if (!s.ok()) {
        Debug(logger_, "Failed resetting zone !");
      }
      continue;
    }

    // A used but not finished zone, and reaches the finish threshold.
//    if ((z->capacity() < (z->max_capacity() * finish_threshold_ / 100))) 
    if (z->max_capacity() - z->capacity() >= 1024 * 1024 * 1024 || 
          z->capacity() < z->max_capacity() * finish_threshold_ / 100) {
      s = z->Finish();
      if (!s.ok()) {
        Debug(logger_, "Failed finishing zone");
      }

      PutActiveIOZoneToken();
      z->zbd()->PutActiveIOZoneToken(); 
    }

    // A not finished zone.
    if (!z->IsFull()) {
      if (finish_victim == nullptr) {
        finish_victim = z;
      } else if (finish_victim->capacity() > z->capacity()) {
        finish_victim = z;
      }
    }
  }

  return finish_victim;
}

Zone* HybridBlockDevice::TryAlreadyOpenZone(bool use_rotational, 
    struct FileHints& fh, bool& lifetime_not_good) {

  Zone* alloc_z = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;

  // Only allocate an empty zone for a non-L0 SST
  if (fh.is_sst() && fh.level > 0) {
    return nullptr; 
  }

  // Choose a WAL zone for the WAL. 
  // If the placement is not H2Zone placement, there will be no WAL zone.
  if (fh.is_wal() && !use_rotational) {
    for (int i = 0; i < (int)io_zones_.size(); i++) {
      const auto z = io_zones_[i];

      if ((!z->open_for_write_) && (z->used_capacity_ > 0) && !z->IsFull()) {
        if (z->rotational()) {
          break;
        }

        if (z->for_wal() && !z->for_cache()) {
          lifetime_not_good = false;
          return z;
        }
      }
    }
  }

  // Choose a normal zone for other files.
  for (int i = 0; i < (int)io_zones_.size(); i++) {
    const auto z = io_zones_[i];

    if ((!z->open_for_write_) && (z->used_capacity_ > 0) && !z->IsFull()) {
      if (use_rotational ^ z->rotational()) {
	if (!use_rotational) break;
	continue;
      }

      // If the zone is WAL zone but the file isn't WAL, do not put together 
      if (z->for_wal() && !fh.is_wal()) {
        continue;
      }

      // If there is an non-L0 SST in the zone, do not put together.
      if (z->is_sst() && z->level() > 0) {
        continue;
      }

      unsigned int diff = GetLifeTimeDiff(z->lifetime(), fh.lifetime);
      if ((!z && diff <= best_diff) || 
          (z && diff < best_diff)) {
	alloc_z = z;
	best_diff = diff;
      }
    }
  }

  if (best_diff >= LIFETIME_DIFF_NOT_GOOD) {
    lifetime_not_good = true;
  }

  return alloc_z;
}

IOStatus HybridBlockDevice::TryFinishVictim(Zone* finish_victim) {
  ZonedBlockDevice* zbd_victim = finish_victim->zbd();
  IOStatus s;

  // The active zone is not enough. Finish the victim.
  if (active_io_zones_.load() == max_nr_active_io_zones_ ||
      zbd_victim->active_io_zones_.load() == 
      zbd_victim->max_nr_active_io_zones_) { 

    s = finish_victim->Finish();

    if (!s.ok()) {
      Debug(logger_, "Failed finishing zone");
    }
    PutActiveIOZoneToken();
    zbd_victim->PutActiveIOZoneToken(); 
  }

  return s;
}

bool HybridBlockDevice::IsHeterogeneous() {
  return (HasSsd() && HasHdd());
}

bool HybridBlockDevice::CheckRotationalTrivial(struct FileHints& fh) {
  return (fh.is_sst() && fh.level >= zops_.level_m); 
}

bool HybridBlockDevice::CheckRotationalSpanDB(struct FileHints& fh) {
  // level_m will change during the workload
  return (fh.is_sst() && fh.level > level_m_);
}

bool HybridBlockDevice::CheckRotationalMutant(struct FileHints& /*fh*/) {
  // Always put to the SSD
  return false;
}

void HybridBlockDevice::CheckSpanDBMaxLevel() {
  double ssd_thpt, hdd_thpt;
  double ssd_space_pct = 
    (double)GetSsdFreeSpaceWoWAL() / GetSsdTotalSpaceWoWAL();

  stat_->ExtractThpt(&ssd_thpt, &hdd_thpt);

  if (ssd_thpt > zops_.ssd_write_bound * 0.65) {
    level_m_--;
    if (level_m_ < -1) level_m_ = -1; 
  }

  if (ssd_thpt < zops_.ssd_write_bound * 0.4) {
    level_m_++;
    if (level_m_ > 4) level_m_ = 4; 
  }

  if (ssd_space_pct < 0.1333 && level_m_ > 1) { // 50 GiB in SpanDB code
    level_m_ = 1;
  } else if (ssd_space_pct < 0.08) {  // 30 GiB in SpanDB code
    level_m_ = -2;
  }
}

bool HybridBlockDevice::CheckRotationalH2Zone(struct FileHints& fh) {
  // migration
  if (fh.is_migrated) {
    return fh.is_rotational;
  }
  
  if (fh.is_blob() || fh.is_wal()) { 
    // Put Blob and WAL in SSD 
    return false;
  } 
  
  if (fh.is_sst()) { // SST files
    level_m_ = zenfs_->GetLevelM();
    if (fh.level != level_m_) {
      // Too high level, use rotational
      return fh.level > level_m_;
    } 
    
    return !zenfs_->CanPutInLevelM(); 
  } 
  
  // Put meta in HDD 
  return true;
}

bool HybridBlockDevice::PreAllocateCheckRotational(struct FileHints& fh) {
  if (HasSsd() && !HasHdd()) {
    return false;
  } 
  if (!HasSsd() && HasHdd()) {
    return true;
  } 

  UpdateFileDistributions(true);

  if (!zops_.enable_data_placement) {
    if (zops_.enable_trivial_data_placement) {
      return CheckRotationalTrivial(fh);
    } 
    
    if (zops_.enable_spandb_data_placement) {
      return CheckRotationalSpanDB(fh);
    }

    if (zops_.enable_mutant_data_placement) {
      return CheckRotationalMutant(fh);
    }

    return false;
  }
  
  return CheckRotationalH2Zone(fh);
}

int HybridBlockDevice::GetSsdEmptyZonesNum() {
  int cnt = 0;
  for (int i = 0; i < (int)io_zones_.size(); i++) {
    const auto z = io_zones_[i];
    if (z->for_wal()) continue;
    if (!z->IsUsed()) {
      if (!z->rotational()) {
        cnt++;
        continue;
      }
    }
  }
  return cnt;
}

void HybridBlockDevice::UpdateFileDistributions(bool print) {
  last_time_capacity_updated_ = elapsed_time_double();
  zenfs_->ComputeCompactionHintedInfo(print);
}

double HybridBlockDevice::elapsed_time_double() {
  return gettime() - start_time_double;
}

WorkloadStatus HybridBlockDevice::status() {
  return stat_->status(); 
}

// Still a toy function
Zone *HybridBlockDevice::AllocateZone(struct FileHints& fh) {
  Zone *alloc_z = nullptr;
  Zone *finish_victim = nullptr;

  int allocated = 0;
  Status s;

  io_zones_mtx_.lock();

  bool use_rotational = PreAllocateCheckRotational(fh);
  bool lifetime_not_good;

  /* Make sure we are below the zone open limit */
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    zone_resources_.wait(lk, [this] {
      if (open_io_zones_.load() < max_nr_open_io_zones_) return true;
      return false;
    });
  }

  for (int i = 0; i < 2; i++) {
    /* Try to fill an already open zone */
    /* Reset any unused zones and finish used zones under capacity treshold */
    for (auto zbd : zbds_) {
      if (!finish_victim && zbd->rotational() == use_rotational) {
	finish_victim = TryResetAndFinish(zbd);
      }
    }


    lifetime_not_good = false;

    /* For WAL, find an empty zone first. */
    if (fh.is_wal()) {
      for (int j = 0; j < (int)io_zones_.size(); j++) {
        const auto z = io_zones_[j];
        ZonedBlockDevice* zbd = z->zbd();

        if (!z->for_wal()) {
          break;
        }

        if (z->open_for_write_ || !z->IsEmpty() || z->for_cache()) {
          continue;
        }

        if (zbd->active_io_zones_.load() >= zbd->max_nr_active_io_zones_) {
          /* Active I/O zones reaches the limit */
          continue;
        }

        if (zbd->open_io_zones_.load() >= zbd->max_nr_open_io_zones_) {
          /* Open I/O zones reaches the limit */
          continue;
        }

        alloc_z = z;
        IncrementActiveZones();
        z->zbd()->IncrementActiveZones();

        allocated = 1;
        break;
      }
    }

    /* Not WAL, or no empty zone for WAL, find an open zone*/
    if (alloc_z == nullptr) {
      alloc_z = TryAlreadyOpenZone(use_rotational, fh, lifetime_not_good);
      allocated = (alloc_z) ? (lifetime_not_good) ? 3 : 2 : 0;
    }

    // No open zone for WAL, find a cache zone for it
    if (alloc_z == nullptr && fh.is_wal()) {
      alloc_z = ReleaseCacheZone(); 
      allocated = (alloc_z) ? 4 : 0;
      if (alloc_z) lifetime_not_good = false;
    }

    /* If we did not find a good match, allocate an empty one */
    if (alloc_z == nullptr || lifetime_not_good) {
      /* If we at the active io zone limit, finish an open zone(if available) with
       * least capacity left */
      if (finish_victim != nullptr) {
        TryFinishVictim(finish_victim);
	finish_victim = nullptr;
      }

      if (active_io_zones_.load() < max_nr_active_io_zones_) {
        for (int j = 0; j < (int)io_zones_.size(); j++) {
          const auto z = io_zones_[j];

          ZonedBlockDevice* zbd = z->zbd();

          if (z->open_for_write_ || !z->IsEmpty()) {
            continue;
          }

          if (z->for_wal() && !fh.is_wal()) {
            /* Not matched: WAL zone */
            continue;
          }

          if (z->rotational() ^ use_rotational) {
            /* Not matched: media type */
            continue;
          }

          if (zbd->active_io_zones_.load() >= zbd->max_nr_active_io_zones_) {
            /* Active I/O zones reaches the limit */
            continue;
          }

          if (zbd->open_io_zones_.load() >= zbd->max_nr_open_io_zones_) {
            /* Open I/O zones reaches the limit */
            continue;
          }

          alloc_z = z;

          IncrementActiveZones();
          zbd->IncrementActiveZones();

          allocated = 1;
          break;
        }
      }
    }

    /* Physically open the zone */
    if (alloc_z) {
      ZonedBlockDevice* zbd = alloc_z->zbd();

      assert(!alloc_z->open_for_write_);
      alloc_z->open_for_write_ = true;

      if (allocated == 1 || alloc_z->used_capacity_ == 0) {
	alloc_z->SetFileLifetime(fh.lifetime);
	alloc_z->SetFileType(fh.file_type);
	alloc_z->SetLevel(fh.level);
      }

      IncrementOpenZones();
      zbd->IncrementOpenZones();

      Debug(logger_,
          "Allocating zone(new=%d) fstart: 0x%lx start: "
          "0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          allocated, alloc_z->fstart(), alloc_z->start(),
          alloc_z->wp(), alloc_z->lifetime(), fh.lifetime);
      // We only modify here for HybridBlockDevice

      if (alloc_z->hbd() == nullptr) {
        alloc_z->SetFzbd(this);
      }

      break;  // Don't need to check other media
    }

    if (fh.is_migrated) {
      break;
    }

    use_rotational = !use_rotational;
  }

  PrintSsdUtil();

  io_zones_mtx_.unlock();
  LogZoneStats();

  if (alloc_z) {
    Info(logger_, "AllocateZone result is %s, zone num = %d\n",
        alloc_z->rotational() ? "hdd" : "ssd", (int)(alloc_z->zone_nr()));
  } else {
    Info(logger_, "AllocateZone result is none\n");
  }

  return alloc_z;
}

void HybridBlockDevice::PrintSsdUtil() {
  std::vector<double> utils;
  double avg = 0.0; //, pct25, pct50, pct75, pct90;
  for (const auto z : io_zones_) {
    if (z->rotational()) break;
    double util = z->util();
    utils.push_back(util);
    avg += util;
  }

  if (utils.size() > 0) {
    avg /= (double)utils.size();
  }
//  std::sort(utils.begin(), utils.end());
}

ZonedBlockDevice* HybridBlockDevice::GetDevice(uint64_t offset) {
  for (uint32_t i = 0; i < zbds_.size(); i++) {
    ZonedBlockDevice* zbd = zbds_[i];
    if (offset >= zbd->fstart() && offset <= zbd->fend()) {
      return zbd;
    }
  }
  return nullptr;
}

IOStatus HybridBlockDevice::PositionedRead(uint64_t offset, size_t n, 
    char* scratch, bool direct, ssize_t* ret, double* latency) {

  /* We may get some unaligned direct reads due to non-aligned extent lengths, so
   * fall back on non-direct-io in that case.
   */

  ZonedBlockDevice* zbd = GetDevice(offset);
  uint64_t dev_off = offset - zbd->fstart();
  double t1, t2;

  t1 = gettime();
  *ret = zbd->PositionedRead(direct, scratch, dev_off, n);
  t2 = gettime();

  if (latency) {
    *latency = t2 - t1;
  }

  if (*ret <= 0) {
    return IOStatus::Corruption("pread error");
  }

  if ((int)(*ret) != (int)n) {
    return IOStatus::Corruption("pread error");
  }

  return IOStatus::OK();
}

bool HybridBlockDevice::CheckOffsetAligned(uint64_t offset) {
  ZonedBlockDevice* zbd = GetDevice(offset);
  uint64_t dev_offset = offset - zbd->fstart();
  return dev_offset % zbd->GetBlockSize() == 0;
}


uint64_t HybridBlockDevice::GetSsdFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones_) {
    if (!z->rotational()) {
      free += z->capacity();
    } 
  }
  return free;
}

uint64_t HybridBlockDevice::GetSsdFreeSpaceWoWAL() {
  uint64_t free = 0;
  for (const auto z : io_zones_) {
    if (!z->rotational() && !z->for_wal()) {
      free += z->capacity();
    } 
  }
  return free;
}

uint64_t HybridBlockDevice::GetHddFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones_) {
    if (z->rotational()) {
      free += z->capacity();
    } 
  }
  return free;
}

uint64_t HybridBlockDevice::GetFreeSpace() {
  return GetSsdFreeSpace() + GetHddFreeSpace();
}

uint64_t HybridBlockDevice::GetSsdTotalSpace() {
  return ssd_total_space_;
}

uint64_t HybridBlockDevice::GetSsdTotalSpaceWoWAL() {
  uint64_t ret = 0;
  for (const auto z : io_zones_) {
    if (!z->rotational() && !z->for_wal()) {
      ret += z->max_capacity();
    } 
    if (z->rotational()) break;
  }
  return ret;
}

uint64_t HybridBlockDevice::GetHddTotalSpace() {
  return hdd_total_space_;
}

int HybridBlockDevice::GetSsdIoZone() {
  int ret = 0;
  for (const auto z : io_zones_) {
    if (z->rotational()) break;
    if (z->for_wal()) continue;
    ret++;
  }
  return ret;
}

int HybridBlockDevice::GetSsdFreeZones() {
  int ret = 0;
  for (const auto z : io_zones_) {
    if (z->rotational()) continue;
    if (z->for_wal()) continue;
    if (z->IsUsed()) continue;
    ret++;
  }
  return ret;
}

uint64_t HybridBlockDevice::GetTotalSpace() {
  return hdd_total_space_ + ssd_total_space_;
}

bool HybridBlockDevice::HasHdd() {
  return (hdd_total_space_ > 0);
}

bool HybridBlockDevice::HasSsd() {
  return (ssd_total_space_ > 0);
}

void HybridBlockDevice::SetZenFSOptions(ZenFSOptions& zenfs_options) {
  zops_ = zenfs_options;
  for (auto& it : zbds_) {
    // TODO deal with the condition when there are multiple SSD/HDD
    it->SetZenFSOptions(zenfs_options);
  }
}


std::vector<uint32_t> HybridBlockDevice::GetBlockSizes() { 
  std::vector<uint32_t> ret;
  for (auto& it : zbds_) {
    ret.push_back(it->GetBlockSize());
  }
  return ret;
}

uint32_t HybridBlockDevice::GetMaximumBlockSize() {
  uint32_t ret = 0;
  for (auto& it : zbds_) {
    if (ret < it->GetBlockSize()) {
      ret = it->GetBlockSize();
    }
  }
  return ret;
}

std::vector<uint32_t> HybridBlockDevice::GetZoneSizesInBlk() {
  std::vector<uint32_t> ret;
  for (auto& it : zbds_) {
    ret.push_back(it->zone_sz_ / it->GetBlockSize());
  }
  return ret;
}

std::vector<uint32_t> HybridBlockDevice::GetNrsZones() {
  std::vector<uint32_t> ret;
  for (auto& it : zbds_) {
    ret.push_back(it->GetNrZones());
  }
  return ret;
}

CacheManager* HybridBlockDevice::cache() {
  return cache_;
}

void HybridBlockDevice::AddStatBytes(bool is_rotational, bool is_read, 
    bool is_compaction_read, bool is_index, int level, uint64_t bytes, 
    double latency, int zone_number) {

  stat_->AddStatBytes(is_rotational, is_read, is_compaction_read, is_index, 
      level, bytes, latency, zone_number);

  if (stat_->printable()) {
    LogZoneUsage();
    PrintStat();
  }
}

void HybridBlockDevice::PrintStat() {
  stat_->PrintStat(elapsed_time_double());
}

void HybridBlockDevice::SetZenFS(ZenFS* zenfs) {
  zenfs_ = zenfs;
  if (cache_) {
    cache_->SetZenFS(zenfs);
  }
}

Zone* HybridBlockDevice::AllocateCacheZone() {
  if (!zops_.enable_cache_manager) {
    return nullptr;
  }

  IOStatus s;

  Zone* z = nullptr;

  if (!ssd_cache_zones_.empty()) {
    Zone* prev_z = ssd_cache_zones_.back(); // Get the latest zone
    prev_z->open_for_write_ = false;

    PutActiveIOZoneToken();
    prev_z->zbd()->PutActiveIOZoneToken();

    prev_z->Finish();
  }

  // We make sure that here all the cache zones are not active

  io_zones_mtx_.lock();
  for (auto it : io_zones_) {
    // Get a new zone for the cache
    if (!it->IsUsed() && it->for_wal() && !it->for_cache()) {
      it->SetForCache();
      ssd_cache_zones_.push(it);
      z = it;

      if (!z->IsFull() && !z->IsEmpty()) {
        PutActiveIOZoneToken();
        z->zbd()->PutActiveIOZoneToken();
      }

      break;
    }

    if (!it->for_wal()) {
      break;
    }
  }
  io_zones_mtx_.unlock();

  // Cannot get any old or new zone, return
  if (ssd_cache_zones_.empty()) {
    return nullptr;
  }

  // Get an old zone 
  if (z == nullptr) {
    z = ssd_cache_zones_.front();
    ssd_cache_zones_.pop();
    ssd_cache_zones_.push(z);
  }

  z->Reset();
  z->open_for_write_ = true;

  IncrementActiveZones();
  z->zbd()->IncrementActiveZones();

  return z;
}

// Assume mtx is held.
Zone* HybridBlockDevice::ReleaseCacheZone() {
  if (!zops_.enable_cache_manager) {
    return nullptr;
  }
  if (ssd_cache_zones_.empty()) {
    return nullptr;
  }
  // Get the oldest zone, and prepare to reset
  // ret can be active (only one cache zone)
  //      or in-active (more than one cache zone)
  Zone* ret = ssd_cache_zones_.front();

  cache_->ClearOldestEntries(ret); 
  ssd_cache_zones_.pop();

  ret->ResetForCache();

  // If full, the token has been already put by previous function
  // If empty, it means that there is only one cache zone.
  if (!ret->IsFull()) {
    PutActiveIOZoneToken();
    ret->zbd()->PutActiveIOZoneToken();
  }

  ret->Reset();

  IncrementActiveZones();
  ret->zbd()->IncrementActiveZones();

  return ret;
}




}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
