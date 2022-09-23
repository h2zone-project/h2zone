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

#define DEFAULT_ZENV_LOG_PATH "/tmp/"

namespace ROCKSDB_NAMESPACE {

// public
void ZenFS::ComputeCompactionHintedInfo(bool print = true) {
  files_mtx_.lock();

  total_lvl_generated_.clear();
  medium_lvl_allocation_.clear();
  lvl_and_output_lvl_to_zones_.clear();
  lvl_generated_.clear();
  wal_sizes_ = 0;

  std::map<int, uint64_t> job_input_processes;
  std::map<int, uint64_t> job_output_process;
  std::map<int, uint64_t> job_sizes;
  std::map<int, int> job_input_num;
  std::map<int, std::map<int, int>> job_input_lvl_num;
  std::map<int, int> job_output_num;
  std::map<int, int> job_input_level;
  std::map<int, int> job_output_level;

  std::map<int, std::set<std::string>> job_input_files;

  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    uint64_t fs = zFile->file_size();
    int lvl = zFile->level();
    if (lvl > 10) {
      if (zFile->is_wal()) {
        wal_sizes_ += fs;
      }
      continue;
    }

    int nzones = (fs) ? (fs / zops_.sst_size + 1) : 0;

    if (zFile->is_compaction_involved() && zFile->is_compaction_input()) {
      // nzones is for input files - data to be generated

      int output_lvl = zFile->compaction_output_level();
      int job_id = zFile->compaction_job();

      lvl_and_output_lvl_to_zones_[lvl][output_lvl] += nzones;
      total_lvl_generated_[output_lvl] += nzones;
      job_input_num[job_id] += nzones;
      job_input_lvl_num[job_id][lvl] += nzones;
      job_input_processes[job_id] += zFile->compaction_process();
      job_sizes[job_id] += fs;
      job_input_level[job_id] = lvl;
      job_output_level[job_id] = output_lvl;
      job_input_files[job_id].insert(zFile->filename());
    }

    if (zFile->is_compaction_involved() && !zFile->is_compaction_input()) {
      // nzones is for output files - data already generated

      int job_id = zFile->compaction_job();

      lvl_generated_[lvl] += nzones;
      job_output_num[job_id] += nzones;
      job_output_process[job_id] += fs;
    }
  }

  // Enumerate each job
  for (auto& it : job_input_processes) {
    int job = it.first;
    double ji = (double)it.second / job_sizes[job] * 100.0;
    double jo = (double)job_output_process[job] / job_sizes[job] * 100.0; 
    int input_lvl = job_input_level[job];
    int output_lvl = job_output_level[job]; 

    bool finished = ((ji - jo) > 90) && (jo == 0.0);
        
    if (print) {
      fprintf(stderr, "job %d: input %.3lf %% output %.3lf %% -----"
          " %d vs %d output_lvl %d %s\n", 
          it.first, ji, jo, 
          job_input_num[job], job_output_num[job], 
          job_output_level[job], finished ? "(fin)" : "");
    }

    if (finished) {
      lvl_and_output_lvl_to_zones_[input_lvl][output_lvl] -= 
        job_input_lvl_num[job][input_lvl];
      total_lvl_generated_[output_lvl] -= job_input_num[job]; 

      if (zops_.enable_aggressive_deletion) {
        for (auto fn : job_input_files[job]) {
          pending_delete_.insert(fn);
        }
      }
    }
  }

  ComputeCurrentAllocation(print);
  ComputeStorageDemands(print);
  ComputeLevelM(print);
  files_mtx_.unlock();
}

int ZenFS::LevelMAvailableZones() {
  int num_available_zones = hbd_->GetSsdIoZone();

  // Subtract the files lower than m
  for (auto& it : level_capacities_) {
    if (it.first >= level_m_) {
      break;
    }
    num_available_zones -= it.second;
  }

  if (medium_lvl_allocation_.count(0)) {
    // Subtract the files in ssd that is higher or equal to m
    for (auto& it : medium_lvl_allocation_[0]) {
      if (it.first >= 10) break;
      if (it.first < level_m_) {
        continue;
      }
      num_available_zones -= it.second;
    }
  }

  return num_available_zones;
}

bool ZenFS::CanPutInLevelM() {
  return LevelMAvailableZones() > 0;
}

// private
void ZenFS::ComputeCurrentAllocation(bool print) {
  double call_times = hbd_->elapsed_time_double();

  std::map<int, int> actual;

  medium_lvl_allocation_.clear();
  current_allocation_.clear();

  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    uint64_t fs = zFile->file_size();
    int lvl = zFile->level();
    int nzones = fs / zops_.sst_size + 1;

    current_allocation_[lvl] += nzones;
    medium_lvl_allocation_[(int)zFile->rotational()][lvl] += nzones;

    actual[lvl]++;
  }

  // print to logger
  {
    for (auto& it0 : current_allocation_) {
      Info(logger_, "(Tf) %.3lf L%d %d\n", call_times, it0.first, it0.second);
    }
    Info(logger_, "-----\n");

    for (auto& it0 : actual) {
      Info(logger_, "(Tf) %.3lf aL%d %d\n", call_times, it0.first, it0.second);
    }
  }

  if (print) {
    fprintf(stderr, "SSD zones: ");
    if (medium_lvl_allocation_.count(0)) {
      for (auto& it0 : medium_lvl_allocation_[0]) {
        fprintf(stderr, "L%d %d - ", it0.first, it0.second);
      }
    }

    fprintf(stderr, "\nHDD zones: ");
    if (medium_lvl_allocation_.count(1)) {
      for (auto& it0 : medium_lvl_allocation_[1]) {
        fprintf(stderr, "L%d %d - ", it0.first, it0.second);
      }
    }
    fprintf(stderr, "\n");
  }
}

void ZenFS::ComputeLevelM(bool print) {
  if (!zops_.enable_data_placement) {
    return;
  } 

  int sum = 0;
  int num_ssd_zones = hbd_->GetSsdIoZone();

  level_m_ = 0;
  for (auto& it : level_capacities_) {
    if (it.first >= BLOB_FILE_LEVEL) {
      break;
    }

    level_m_ = it.first;
    sum += it.second;
    if (sum >= num_ssd_zones) {
      (print) ? fprintf(stderr, "break lm %d sum %d (ssd cap %d)\n", 
            level_m_, sum, num_ssd_zones) : 0;
      break;
    }
  }

  if (sum < num_ssd_zones) {
    level_m_ += 2;
  }

  if (level_m_ == 0) {
    (print) ? fprintf(stderr, "lm revert from 0 to 1 (ssd cap %d)\n", 
        num_ssd_zones) : 0;
    level_m_ = 1;
  } else {
    (print) ? fprintf(stderr, "lm selected %d (ssd cap %d)\n", 
        level_m_, num_ssd_zones) : 0;
  }
}

void ZenFS::ComputeStorageDemands(bool print) {
  level_capacities_.clear();
  storage_demands_.clear();

  // print
  if (print) {
    fprintf(stderr, " wal tot size %.3lf MiB\n", 
        (double)wal_sizes_ / 1024.0 / 1024.0);
    for (auto& it : current_allocation_) {
      // input level
      int lvl = it.first;
      fprintf(stderr, "(Tf) %.3lf L%d %d comp: ", 
          hbd_->elapsed_time_double(), lvl, it.second);

      for (auto& it1 : lvl_and_output_lvl_to_zones_[lvl]) {
        // output level and the number of zones
        fprintf(stderr, "[o%d %d] ", it1.first, it1.second);
      }

      fprintf(stderr, " gen'ed %d\n", lvl_generated_[lvl]);
    }
    fprintf(stderr, "------------\n");
  }

  storage_demands_[0] = total_lvl_generated_[0] - lvl_generated_[0] +  
    ((wal_sizes_ > 0) ? 
     (wal_sizes_ / zops_.sst_size + 1) : 0);
  level_capacities_[0] = current_allocation_[0] + storage_demands_[0]; 

  total_storage_demands_ = storage_demands_[0];

  for (auto& it : current_allocation_) {
    int lvl = it.first;
    if (lvl > 0) {
      storage_demands_[lvl] = 
        total_lvl_generated_[lvl] - lvl_generated_[lvl];
      level_capacities_[lvl] = 
        current_allocation_[lvl] + storage_demands_[lvl];
      total_storage_demands_ += storage_demands_[lvl];
    }
  }

  // print capacities
  if (print) {
    fprintf(stderr, "capacities: ");
    for (auto& it : level_capacities_) {
      fprintf(stderr, "[L%d %d (%d)] ", it.first, it.second, 
          storage_demands_[it.first]);
    }
    fprintf(stderr, "\n");
  }
}

};  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
