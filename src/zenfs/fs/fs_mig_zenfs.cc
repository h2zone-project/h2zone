#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include "fs_zenfs.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <utility>
#include <vector>

#include "rocksdb/utilities/object_registry.h"
#include "util/crc32c.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

void ZenFS::MigrationDaemonRun() {
  bool flag;
  while (daemon_stop_ == false) {
    usleep(500000);
    
    // Check Explicit migration

    if (tb_->AcquireToken()) {
      ComputeCompactionHintedInfo(false);
      flag = MaybePopularityCalibration();
      if (!flag) {
        flag = MaybeCapacityCalibration();
      }
    }
  }
}

// Compute the priority to be in the SSD 
// Should not be empty.
// -1: f1 < f2
// 0: f1 == f2
// 1: f1 > f2
int ZenFS::ComparePriority(ZoneFile* f1, ZoneFile* f2) {
  // Undefined behavior
  if (f1 == nullptr) {
    return -1;
  } else if (f2 == nullptr) {
    return 1;
  }

  // lower is better
  if (f1->level() != f2->level()) { 
    if (f1->file_type() == kWalFile) {
      return 1;
    } else if (f2->file_type() == kWalFile) {
      return -1;
    }
    return (f1->level() > f2->level()) ? -1 : 1; 
  }

  // not moved is better
  if (f1->trivial_moved() != f2->trivial_moved()) {  
    return (f1->trivial_moved()) ? -1 : 1;
  } 

  // higher is better
  if (f1->rand_read_times_.load() != f2->rand_read_times_.load()) {
    int rt1 = f1->rand_read_times_.load();
    int rt2 = f2->rand_read_times_.load();
    return (rt1 > rt2 * 1.01) ? 1 : (rt1 * 1.01 < rt2) ? -1 : 0;
  }

//  // lower is better
//  if (z1->lifespan() != z2->lifespan()) {
//    double l1 = z1->lifespan();
//    double l2 = z2->lifespan();
//    return (l1 > l2 * 2.0) ? -1 : (l1 * 2.0 < l2) ? 1 : 0;
//  }

  return 0;
}

ZoneFile* ZenFS::TrySelectSsdObject() {
  ZoneFile* ret = nullptr;

  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    if (zFile->rotational()) continue;
    if (zFile->IsOpenForWR()) continue;
    if (!zFile->is_sst()) continue;
    if (zFile->IsBeingMigrated()) continue;

    if (!ret || ComparePriority(ret, zFile) > 0) {
      ret = zFile;

      if (zops_.print_migration) {
        fprintf(stderr, "ssd zf %d rot %d"
            " lvl %d read-times %d\n", 
            (int)zFile->id(), (int)zFile->rotational(), 
            (int)zFile->level(), (int)zFile->rand_read_times_.load());
      }
    }
  }

  return ret;
}

ZoneFile* ZenFS::TrySelectHddObject() {
  ZoneFile* ret = nullptr;

  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    if (!zFile->rotational()) continue;
    if (zFile->IsOpenForWR()) continue;
    if (!zFile->is_sst()) continue;
    if (zFile->IsBeingMigrated()) continue;

    if (!ret || ComparePriority(ret, zFile) < 0) {
      ret = zFile;

      if (zops_.print_migration) {
        fprintf(stderr, "hdd zf %d rot %d"
            " lvl %d read-times %d\n", 
            (int)zFile->id(), (int)zFile->rotational(), 
            (int)zFile->level(), (int)zFile->rand_read_times_.load());
      }
    }
  }

  return ret;
}

bool ZenFS::MaybeCapacityCalibration() {
  if (!hbd_->IsHeterogeneous()) {
    return false;
  }
  if (hbd_->elapsed_time_double() < 32.0) {
    return false;
  }
  if (!zops_.enable_explicit_migration) {
    return false;
  }
  if (!zops_.enable_capacity_calibration) {
    return false;
  }
  if (daemon_stop_.load()) {
    return false;
  }

  ZoneFile* s_zf = nullptr;

  s_zf = TrySelectSsdObject();
  if (s_zf && 
      (s_zf->level() < level_m_ ||
      (s_zf->level() == level_m_ && LevelMAvailableZones() >= 0))) {
    s_zf = nullptr;
  }

  if (s_zf) {
    Status s = PerformMigration(s_zf, true); // To rotational 

    if (s.ok()) {
      return true;
    } else {
      return false;
    }
  }

  return false;
}

bool ZenFS::MaybePopularityCalibration() {
  if (hbd_->status() != WorkloadStatus::kReadIntensive) {
    return false;
  }
  if (!hbd_->IsHeterogeneous()) {
    return false;
  }
  if (!zops_.enable_explicit_migration) {
    return false;
  }
  if (daemon_stop_.load()) {
    return false;
  }

  if (zops_.enable_data_placement) {
    return H2ZoneStylePopularityMigration();
  } else if (zops_.enable_trivial_data_placement) {
    return TrivialStylePopularityMigration();
  }
  return false;
}

bool ZenFS::H2ZoneStylePopularityMigration() {
  // The storage demands of low levels
  int sd_low_levels = 0;
  for (auto& it : storage_demands_) {
    int lvl = it.first;
    if (lvl < level_m_) {
      sd_low_levels += it.second;
    }
  }

  MigrationStatus ms = MigrationStatus::kOnGoing;

  // 1. Find an HDD ZoneFile to move to the SSD 
  if (hbd_->GetSsdFreeZones() > sd_low_levels) {
    ms = SelectAndMoveHddToSsd(ms);
  } else {
    // 2. Swap an SST in the SSD with an SST in the HDD
    //    Check the priority first
    ZoneFile* sf_object = TrySelectSsdObject();
    ZoneFile* hf_object = TrySelectHddObject();

    if (!sf_object || !hf_object ||
       ComparePriority(sf_object, hf_object) >= 0) {
      ms = MigrationStatus::kAborted;
    }

//    if (ms == MigrationStatus::kOnGoing) {
      ms = SelectAndMoveSsdToHdd(ms);
//      if (hbd_->GetSsdFreeZones() <= sd_low_levels) {
//        ms = MigrationStatus::kAborted;
//      }
//      ms = SelectAndMoveHddToSsd(ms);
//    }
  }

  return (ms == MigrationStatus::kFinished);
}

// Only one line different from the H2Zone style
bool ZenFS::TrivialStylePopularityMigration() {
  MigrationStatus ms = MigrationStatus::kOnGoing;

  // 1. Find an HDD ZoneFile to move to the SSD 
  if (hbd_->GetSsdFreeZones() > 0) {
    ms = SelectAndMoveHddToSsd(ms, level_m_ - 1);

  } else {
    // 2. Swap an SST in the SSD with an SST in the HDD
    //    Check the priority first
    ZoneFile* sf_object = TrySelectSsdObject();
    ZoneFile* hf_object = TrySelectHddObject();

    if (!sf_object || !hf_object ||
       ComparePriority(sf_object, hf_object) >= 0) {
      ms = MigrationStatus::kAborted;
    }

//    if (ms == MigrationStatus::kOnGoing) {
      ms = SelectAndMoveSsdToHdd(ms);
//      if (hbd_->GetSsdFreeZones() <= 0) {
//        ms = MigrationStatus::kAborted;
//      }
//      ms = SelectAndMoveHddToSsd(ms, level_m_ - 1);
//    }
  }

  return ms == MigrationStatus::kFinished;
}

//     (sf_object && sf_object->level() > hf_object->level());

// Need to make sure that the SSD space is enough
ZenFS::MigrationStatus ZenFS::SelectAndMoveHddToSsd(MigrationStatus ms, 
    int max_level) {
  if (ms != MigrationStatus::kOnGoing) {
    return ms;
  }

  ZoneFile* hf_object = TrySelectHddObject();
  if (!hf_object) { // There are no SSTs in the HDD, no need
    return MigrationStatus::kAborted;
  }

  // Check the condition for H2Zone data placement
  bool flag_hdd_to_ssd = hf_object->level() <= max_level; 

  if (flag_hdd_to_ssd) {
    Status s = PerformMigration(hf_object, false);
    if (s.ok()) {
      return MigrationStatus::kFinished;
    } 
    return MigrationStatus::kAborted;
  }

  return MigrationStatus::kOnGoing;
}

// Assume that the HDD space is enough
ZenFS::MigrationStatus ZenFS::SelectAndMoveSsdToHdd(MigrationStatus ms) {
  if (ms != MigrationStatus::kOnGoing) {
    return ms;
  }

  ZoneFile* sf_object = TrySelectSsdObject();
  if (!sf_object) { // There are no SSTs in the SSD, no need
    return MigrationStatus::kAborted;
  }

  // Check the condition for H2Zone data placement
//  bool flag_hdd_to_ssd = zops_.enable_data_placement || 
//    (zops_.enable_trivial_data_placement && 
//     sf_object->level() < level_m_); 

//  if (flag_hdd_to_ssd) {
    Status s = PerformMigration(sf_object, true);
    if (s.ok()) {
      return MigrationStatus::kFinished;
    } 
    return MigrationStatus::kAborted;
//  }
//
//  return MigrationStatus::kOnGoing;
}


Status ZenFS::PerformMigration(ZoneFile* zf, bool to_rotational) {
  bool has_token = false;
  Status s = zf->BeginMigration(to_rotational);
  if (!s.ok()) {
    return Status::Aborted("begin deleted (before begining migration)");
  }

  uint64_t migrate_size = 2 * 1024 * 1024;
  uint64_t migration_time_per_sec = 
    zops_.explicit_throughput / migrate_size;
  if (zops_.explicit_throughput < migrate_size) {
    migrate_size = zops_.explicit_throughput;
    migration_time_per_sec = 1;
  }

  TokenBucket tb(1, migration_time_per_sec);

  double time1 = hbd_->elapsed_time_double();
  double time2;

  while (zf->MigrationNotFinished()) {
    if (zf->IsBeingDeleted()) {
      zf->EndMigration("being deleted");
      return Status::Aborted();
    }

    // TODO mutant does not consider rate-limiting
    while (true) {
      has_token = tb.AcquireToken();
      if (has_token) break;
      usleep((int)(5000));  
    }
    
    s = zf->Move(migrate_size);
    if (!s.ok()) {
      // Stop migration
      zf->EndMigration("unknown");
      return Status::Aborted();
    }

    time2 = hbd_->elapsed_time_double();
    if (time2 - time1 >= 15.0 || zf->SizeLeftForMigration() == 0) {
      time1 = time2;
    }
  }

  zf->EndMigration();
  return Status::OK();
}

};  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
