// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <unistd.h>

#include <string>
#include <sstream>
#include <fstream>
#include <utility>
#include <vector>

#include <linux/fs.h>

#include "util.h"
#include "io_zenfs.h"
#include "rocksdb/env.h"

#define BLKDISCARD _IO(0x12,119)

namespace ROCKSDB_NAMESPACE {

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones_)
    if (z->start() <= offset && offset < (z->start() + zone_sz_)) return z;
  return nullptr;
}

void ZonedBlockDevice::FindFileSystemPath(std::string bdevname) {
  // 1. Find device name
  std::string delimiter = ":", token, str = zops_.posix_device;
  size_t pos = 0;
  int p = -1, i = 0;
  while ((pos = str.find(delimiter)) != std::string::npos) {
      token = str.substr(0, pos);
      if (token == bdevname) {
        // Find a device that uses file system
        fs_ = FileSystem::Default();
        p = i;
      }
      str.erase(0, pos + delimiter.length());
      i++;
  }

  if (str == bdevname) {
    // Find a device that uses file system
    fs_ = FileSystem::Default();
    p = i;
  }

  // 2. Find path 
  str = zops_.posix_file_path;
  i = 0;
  while (p >= 0 && (pos = str.find(delimiter)) != std::string::npos) {
      token = str.substr(0, pos);
      if (i == p) {
        posix_file_path_ = token;
      }

      str.erase(0, pos + delimiter.length());
      i++;
  }

  if (i == p) {
    posix_file_path_ = str;
  }
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname,
                                   std::shared_ptr<Logger> logger)
    : filename_("/dev/" + bdevname), logger_(logger) {
  Info(logger_, "New Zoned Block Device: %s", filename_.c_str());
  zops_.Read();

  FindFileSystemPath(bdevname);
};

std::string ZonedBlockDevice::ErrorToString(int err) {
  char *err_str = strerror(err);
  if (err_str != nullptr)
    return std::string(err_str);
  return "";
}

IOStatus ZonedBlockDevice::CheckScheduler() {
  std::ostringstream path;
  std::string s = filename_;
  std::fstream f;

  s.erase(0, 5); // Remove "/dev/" from /dev/nvmeXnY
  path << "/sys/block/" << s << "/queue/scheduler";
  f.open(path.str(), std::fstream::in);
  if (!f.is_open()) {
    return IOStatus::InvalidArgument("Failed to open " + path.str());
  }

  std::string buf;
  getline(f, buf);
  if (buf.find("[mq-deadline]") == std::string::npos) {
    f.close();
    return IOStatus::InvalidArgument("Current ZBD scheduler " + s + 
        " is not mq-deadline, set it to mq-deadline.");
  }

  f.close();
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::ReadRotational() {
  char fn[200];
  std::string dev_name = filename_;
  dev_name.erase(0, 5);
  sprintf(fn, "/sys/block/%s/queue/rotational", dev_name.c_str());

  FILE* fp = fopen(fn, "r");
  if (!fp) {
    return IOStatus::InvalidArgument("Cannot open the rotational file");
  }
  int rotational = 0;
  int ret = fscanf(fp, "%d", &rotational);
  if (!ret) {
    return IOStatus::InvalidArgument("Cannot read the rotational file");
  }
  rotational_ = (rotational > 0);

  fclose(fp);

  return IOStatus::OK();
} 

IOStatus ZonedBlockDevice::ReadZoned() {
  char fn[200];
  char zoned[200];
  int ret;

  std::string dev_name = filename_;
  dev_name.erase(0, 5);
  sprintf(fn, "/sys/block/%s/queue/zoned", dev_name.c_str());

  FILE* fp = fopen(fn, "r");
  if (!fp) {
    return IOStatus::InvalidArgument("Cannot open the zoned file");
  }
  ret = fscanf(fp, "%s", zoned);
  if (!ret) {
    return IOStatus::InvalidArgument("Cannot read the zoned file");
  }
  fclose(fp);

  if (!strcmp(zoned, "host-managed")) {
    zoned_ = true;
  } else {
    zoned_ = false;
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::ZonedDeviceOpen(bool readonly) {
  zbd_info info;

  read_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  if (read_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device: " + ErrorToString(errno));
  }

  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_DIRECT, &info);
  if (read_direct_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device: " + ErrorToString(errno));
  }

  if (readonly) {
    write_f_ = -1;
  } else {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT | O_EXCL, &info);
    if (write_f_ < 0) {
      return IOStatus::InvalidArgument("Failed to open zoned block device: " + ErrorToString(errno));
    }
  }

  if (info.model != ZBD_DM_HOST_MANAGED) {
    return IOStatus::NotSupported("Not a host managed block device");
  }

  if (info.nr_zones < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported(
        "To few zones on zoned block device (32 required)");
  }

  IOStatus ios = CheckScheduler();
  if (ios != IOStatus::OK())
    return ios;

  /* open and get the handler */
  block_sz_ = info.pblock_size;
  zone_sz_ = info.zone_size;
  nr_zones_ = info.nr_zones;

  /* We need one open zone for meta data writes, the rest can be used for files */
  if (info.max_nr_active_zones == 0)
//    max_nr_active_io_zones_ = info.nr_zones;
    max_nr_active_io_zones_ = info.nr_zones;
  else
    max_nr_active_io_zones_ = info.max_nr_active_zones;

  if (info.max_nr_open_zones == 0)
//    max_nr_open_io_zones_ = info.nr_zones;
    max_nr_open_io_zones_ = info.nr_zones;
  else
    max_nr_open_io_zones_ = info.max_nr_open_zones;

  uint64_t constrained_space = 0;
  if (rotational_ && zops_.constrained_hdd_space > 0) {
    constrained_space = zops_.constrained_hdd_space;
  } else if (!rotational_ && zops_.constrained_ssd_space > 0) {
    constrained_space = zops_.constrained_ssd_space;
  }

  // Constrain the space by the option
  if (constrained_space > 0) {
    nr_zones_ = (nr_zones_ > constrained_space / zone_sz_) ? 
      constrained_space / zone_sz_ : nr_zones_;
  }

  Info(logger_, "Zone block device nr zones: %u(%u) max active: %u max open: %u \n",
       info.nr_zones, nr_zones_,
       info.max_nr_active_zones, info.max_nr_open_zones);

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::BlockDeviceOpen(bool readonly) {
  uint64_t devsize;

  read_f_ = open(filename_.c_str(), O_RDONLY);
  if (read_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open block device: " + ErrorToString(errno));
  }

  read_direct_f_ = open(filename_.c_str(), O_RDONLY | O_DIRECT);
  if (read_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open block device: " + ErrorToString(errno));
  }

  if (readonly) {
    write_f_ = -1;
  } else {
    write_f_ = open(filename_.c_str(), O_WRONLY | O_DIRECT);
    if (write_f_ < 0) {
      return IOStatus::InvalidArgument("Failed to open block device (write): " + ErrorToString(errno));
    }
  }

  // Default for non-zoned block device
  block_sz_ = 4096;
  zone_sz_ = 2048ull << 20;  // But capacity is still 1077
  zone_cap_ = 1077ull << 20;

  if (ioctl(write_f_, BLKGETSIZE64, &devsize)) {
    return IOStatus::InvalidArgument("Failed to get size of block device: " + ErrorToString(errno));
  } 
  nr_zones_ = devsize / zone_cap_;
 
  max_nr_active_io_zones_ = 14; // nr_zones_;
  max_nr_open_io_zones_ = 14; // nr_zones_;

  uint64_t constrained_space = 0;
  if (rotational_ && zops_.constrained_hdd_space > 0) {
    constrained_space = zops_.constrained_hdd_space;
  } else if (!rotational_ && zops_.constrained_ssd_space > 0) {
    constrained_space = zops_.constrained_ssd_space;
  }

  // Constrain the space by the option
  if (constrained_space > 0) {
    nr_zones_ = (nr_zones_ > constrained_space / zone_sz_) ? 
      constrained_space / zone_sz_ : nr_zones_;
  }

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       nr_zones_, max_nr_active_io_zones_, max_nr_open_io_zones_);

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::FileSystemOpen(bool /*readonly*/) {
  block_sz_ = 4096;
  zone_sz_ = 256ull << 20;  
  zone_cap_ = 256ull << 20;

  uint64_t constrained_space = 0;
  if (rotational_ && zops_.constrained_hdd_space > 0) {
    constrained_space = zops_.constrained_hdd_space;
  } else if (!rotational_ && zops_.constrained_ssd_space > 0) {
    constrained_space = zops_.constrained_ssd_space;
  }

  nr_zones_ = constrained_space / zone_cap_;
  max_nr_active_io_zones_ = nr_zones_;
  max_nr_open_io_zones_ = nr_zones_;

  Info(logger_, "File system nr zones: %u\n", nr_zones_);

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::ZonedAddZones(bool readonly) {
  unsigned int reported_zones;
  uint64_t addr_space_sz = (uint64_t)nr_zones_ * zone_sz_;
  uint64_t m = 0, i = 0;

  struct zbd_zone *zone_rep;

  int ret = zbd_list_zones(read_f_, 0, addr_space_sz, ZBD_RO_ALL, &zone_rep,
                       &reported_zones);

  if (ret) {
    Error(logger_, "Failed to list zones, err: %d", ret);
    return IOStatus::IOError("Failed to list zones");
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  while (m < ZENFS_META_ZONES && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) != ZBD_ZONE_TYPE_SWR) continue; 
    if (zbd_zone_offline(z)) continue; 

    meta_zones_.push_back(new Zone(this, z));
    if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z) ||
        zbd_zone_closed(z)) {
      active_io_zones_++;
    }
    m++;
  }

  for (; i < reported_zones; i++) {
    struct zbd_zone *z = &zone_rep[i];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) != ZBD_ZONE_TYPE_SWR) continue;
    if (zbd_zone_offline(z)) continue;

    Zone *newZone = new Zone(this, z);
    io_zones_.push_back(newZone);
    if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z) ||
        zbd_zone_closed(z)) {
      active_io_zones_++;
      if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z)) {
        if (!readonly) {
          newZone->Close();
        }
      }
    }
  }

  free(zone_rep);

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::ConvAddZones() {
  uint64_t m = 0, i = 0;
  uint64_t offset = 0;

  std::map<uint64_t, uint64_t> wps;
  active_io_zones_ = 0;
  open_io_zones_ = 0;

  {
    FILE* fp = fopen("/tmp/conv_wp", "r");
    uint64_t start, wp;

    if (fp == NULL) {
      fprintf(stderr, "Cannot read conv wp file, error %d %s\n",
          errno, strerror(errno));
    } else {
      while (fscanf(fp, "%ld%ld", &start, &wp) == 2) {
        wps[start] = wp;
        if (wp - start < zone_cap_) {
          active_io_zones_++;
        }
      }
    }
  }

  while (m < ZENFS_META_ZONES && i < nr_zones_) {
    meta_zones_.push_back(new Zone(this, offset, zone_cap_, 
          wps.count(offset) ? wps[offset] : 0));
    offset += zone_cap_;
    m++;
    i++;
  }

  for (; i < nr_zones_; i++) {
    Zone* z = new Zone(this, offset, zone_cap_, 
        wps.count(offset) ? wps[offset] : 0);
    offset += zone_cap_;

    io_zones_.push_back(z);
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::FileSystemAddZones() {
  uint64_t m = 0, i = 0;
  uint64_t offset = 0;

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  while (m < ZENFS_META_ZONES && i < nr_zones_) {
    Zone* z = new Zone(this, fs_, posix_file_path_, offset, zone_cap_); 
    meta_zones_.push_back(z);

    offset += zone_cap_;
    m++;
    i++;

    if (!z->IsFull() && !z->IsEmpty()) {
      active_io_zones_++;
    }
  }

  for (; i < nr_zones_; i++) {
    Zone* z = new Zone(this, fs_, posix_file_path_, offset, zone_cap_); 
    io_zones_.push_back(z);

    offset += zone_cap_;

    if (!z->IsFull() && !z->IsEmpty()) {
      active_io_zones_++;
    }
  }

  return IOStatus::OK();
}

void ZonedBlockDevice::CheckZones() {
  for (int i = 0; i < (int)meta_zones_.size(); i++) {
    Zone* z = meta_zones_[i];
    fprintf(stderr, "%ld + %ld (MiB) ", 
        z->start() / (1 << 20), z->max_capacity() / (1 << 20)); 
    fprintf(stderr, "wp at %ld MiB %ld\n", 
        z->wp() / (1 << 20), z->wp() % (1 << 20));
  }

  fprintf(stderr, "---------\n");

  for (int i = 0; i < (int)io_zones_.size(); i++) if (i<3) {
    Zone* z = io_zones_[i];
    fprintf(stderr, "%ld + %ld (MiB) ", 
        z->start() / (1 << 20), z->max_capacity() / (1 << 20)); 
    fprintf(stderr, "wp at %ld MiB %ld\n", 
        z->wp() / (1 << 20), z->wp() % (1 << 20));
  }
}

ssize_t ZonedBlockDevice::PositionedRead(bool direct, char* scratch, 
    uint64_t offset, size_t n) {
  if (!fs_) {
    // zoned or not zoned device
    int f = GetReadFD();
    int f_direct = GetReadDirectFD();
    bool aligned = (n % GetBlockSize() == 0);
    ssize_t ret;

    if (direct & aligned) {
      ret = pread(f_direct, scratch, n, offset);
    } else {
      ret = pread(f, scratch, n, offset);
    }

    if (zops_.log_zf_read_hdd && rotational()) {
      Info(logger_, " %ld %ld hdd read trace\n", offset, n);
    }

    return ret;
  }

  IOStatus s;
  Zone* pz = nullptr;

  if (zops_.print_posix_rw) {
    fprintf(stderr, "zbd: file system read %s offset %ld size %ld\n", 
        filename_.c_str(), offset, n);
  }

  for (int i = 0; i < (int)meta_zones_.size(); i++) {
    Zone* z = meta_zones_[i];
    if (offset >= z->start() && offset < z->start() + z->max_capacity()) {
      pz = z;
      break;
    } 
  } 

  for (int i = 0; i < (int)io_zones_.size() && pz == nullptr; i++) {
    Zone* z = io_zones_[i];
    if (offset >= z->start() && offset < z->start() + z->max_capacity()) {
      pz = z;
      break;
    } 
  } 

  if (pz == nullptr) {
    fprintf(stderr, "fs: cannot find zone for device %s offset %ld\n", 
        filename_.c_str(), offset); 
    return 0;
  }

  return pz->PositionedRead(scratch, offset, n);
}

IOStatus ZonedBlockDevice::Open(bool readonly) {
  IOStatus ios;

  /* Check whether SSD (not rotational) or HDD (rotational) */
  if (!(ios = ReadRotational()).ok()) {
    fprintf(stderr, "Read rotational failed %s\n", 
        ios.ToString().c_str());
    exit(1);
  }
  if (!(ios = ReadZoned()).ok()) {
    fprintf(stderr, "Read zoned failed %s\n", 
        ios.ToString().c_str());
    exit(1);
  }

  if (zoned_) {
    ios = ZonedDeviceOpen(readonly);
  } else if (!fs_) { 
    ios = BlockDeviceOpen(readonly);
  } else {
    ios = FileSystemOpen(readonly);
  }
  if (!ios.ok()) {
    fprintf(stderr, "Open zbd failed %s\n", 
        ios.ToString().c_str());
    exit(1);
  }

  if (zoned_) {
    ios = ZonedAddZones(readonly);
  } else if (!fs_) {
    ios = ConvAddZones();
    CheckZones();
  } else {
    ios = FileSystemAddZones();
    CheckZones();
  }

  if (!ios.ok()) {
    fprintf(stderr, "Add zones failed %s\n", 
        ios.ToString().c_str());
    exit(1);
  }

  start_time_ = time(NULL);

  return IOStatus::OK();
}

void ZonedBlockDevice::PutActiveIOZoneToken() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  active_io_zones_--;
  fprintf(stderr, "PutActiveIOZoneToken active_io_zones_ %ld %s\n", 
      active_io_zones_.load(), filename_.c_str()); 
  if (active_io_zones_ < 0) {
    fprintf(stderr, "%s active_io_zones_ should be non-negative\n", 
        filename_.c_str());
    exit(1);
  }
  zone_resources_.notify_one();
}

void ZonedBlockDevice::PutOpenIOZoneToken() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  open_io_zones_--;
  fprintf(stderr, "PutOpenIOZoneToken open_io_zones_ %ld %s\n", 
      open_io_zones_.load(), filename_.c_str()); 
  if (open_io_zones_ < 0) {
    fprintf(stderr, "%s open_io_zones_ should be non-negative\n", 
        filename_.c_str());
    exit(1);
  }
  zone_resources_.notify_one();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones_) {
    free += z->capacity();
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones_) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable= 0;
  for (const auto z : io_zones_) {
    if (z->IsFull())
      reclaimable += (z->max_capacity() - z->used_capacity_);
  }
  return reclaimable;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;
  io_zones_mtx_.lock();

  for (const auto z : io_zones_) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity() - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity();
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());

  io_zones_mtx_.unlock();
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones_) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start(), used, used / MB);
    }
  }
}

ZonedBlockDevice::~ZonedBlockDevice() {
  if (zoned_) {
    zbd_close(read_f_);
    zbd_close(read_direct_f_);
    zbd_close(write_f_);
  }
}

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;

  return LIFETIME_DIFF_NOT_GOOD;
}

Zone *ZonedBlockDevice::AllocateMetaZone() {
  for (const auto z : meta_zones_) {
    /* If the zone is not used, reset and use it */
    if (!z->IsUsed()) {
      if (!z->IsEmpty()) {
        if (!z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          continue;
        }
      }
      printf("meta zone\n");
      return z;
    }
  }
  return nullptr;
}

void ZonedBlockDevice::ResetUnusedIOZones() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  /* Reset any unused zones */
  for (const auto z : io_zones_) {
    if (!z->IsUsed() && !z->IsEmpty()) {
      if (!z->IsFull()) PutActiveIOZoneToken(); 
      if (!z->Reset().ok()) Warn(logger_, "Failed reseting zone");
    }
  }
}

/* Abandoned - Use HybridBlockDevice::AllocateZone() */
Zone *ZonedBlockDevice::AllocateZone(struct FileHints&) {
  return nullptr;
} 

/* Extracted from AllocateZone() 
   The boundary of active I/O zones is checked by FZBD::AllocateZone()
 */
void ZonedBlockDevice::IncrementActiveZones() {
  active_io_zones_++;
}

/* Extracted from AllocateZone() */
void ZonedBlockDevice::IncrementOpenZones() {
  /* Make sure we are below the zone open limit */
//  {
//    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
//    zone_resources_.wait(lk, [this] {
//      if (open_io_zones_.load() < max_nr_open_io_zones_) return true;
//      return false;
//    });
//  }
  open_io_zones_++;
}

void ZonedBlockDevice::SetZenFSOptions(ZenFSOptions& zenfs_options) {
  zops_ = zenfs_options;
}

ZenFSOptions ZonedBlockDevice::zenfs_options() {
  return zops_;
}

std::string ZonedBlockDevice::filename() { return filename_; }
uint32_t ZonedBlockDevice::GetBlockSize() { return block_sz_; }

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
