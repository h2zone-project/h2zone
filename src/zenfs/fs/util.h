#pragma once

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include <sys/time.h>

#include "zenfs_options.h"
#include <map>
#include <vector>

#define BLOB_FILE_LEVEL 10

namespace ROCKSDB_NAMESPACE {

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

double gettime();

class HybridBlockDevice;

enum WorkloadStatus { kNormal, kReadIntensive, kWriteIntensive };

class TokenBucket {
 public:
  TokenBucket(int second, int token_each_time);
  bool AcquireToken(); 

 private:
  double time1_;
  double time2_;
  int second_ = 0;
  const int acquire_interval = 50;
  int tokens_ = 0;
  int token_each_time_ = 1;
};

class ZenStat {
 private:
  bool has_ssd = true;
  bool has_hdd = true;

  std::mutex stat_mtx_;

  std::vector<uint64_t> total_cnt;
  std::vector<uint64_t> total_bytes;

  uint64_t total_rwc = 0; //() { return total(total_cnt, {0, 1, 2, 3, 4, 5, 6, 7}); }
  uint64_t total_rwb = 0; //() { return total(total_bytes, {0, 1, 2, 3, 4, 5, 6, 7}); }

  int max_level = 0;

  uint64_t hdd_w = 0;  // bytes of HDD writes (all)
  uint64_t ssd_w = 0;
  uint64_t hdd_r = 0;  // # of HDD reads, but not compaction read and not index read
  uint64_t ssd_wal_w = 0;

  double time1 = 0.0;

  std::map<int, uint64_t> ssd_level_2_read_bytes;
  std::map<int, uint64_t> hdd_level_2_read_bytes;
  std::map<int, uint64_t> ssd_level_2_compaction_read_bytes;
  std::map<int, uint64_t> hdd_level_2_compaction_read_bytes;
  std::map<int, uint64_t> ssd_level_2_write_bytes;
  std::map<int, uint64_t> hdd_level_2_write_bytes;

  // non-comp ssd read, comp ssd read, non-wal ssd write, wal ssd write
  // non-comp hdd read, comp hdd read, non-wal hdd write, wal hdd write
  std::vector<std::map<double, std::vector<int>>> latencies;
  std::map<double, std::pair<uint64_t, int>> lat2hddzone;

  WorkloadStatus ws = WorkloadStatus::kNormal;

  bool printable_ = false;

  ZenFSOptions zops_;

  // For spandb
  uint64_t hdd_rwtotal = 0, ssd_rwtotal = 0;
  double extract_ts = 0.0;

  double hdd_latency_total = 0.0;
  uint64_t hdd_bytes_total = 0;
  uint64_t hdd_read_times = 0;
  std::map<int, int> hdd_read_times_lvl;

 public:
  ZenStat(ZenFSOptions& zops);
  void SetZenFSOptions(ZenFSOptions& zops);
  int index(bool, bool, bool, int);
  void AddStatBytes(bool, bool, bool, bool, int, uint64_t, double, int);
  void ExtractThpt(double*, double*);
  void PrintLatencies(std::vector<int> indices);
  void CheckWorkload();
  void LogLevelBytes(Logger* logger);
  void PrintStat(double time);
  int max_lvl() { return max_level; } 

  uint64_t total(std::vector<uint64_t>& v, std::vector<int> indices);
  uint64_t total_rc(); //{ return total(total_cnt, {0, 1, 4, 5}); }
  uint64_t total_wc(); //{ return total(total_cnt, {2, 3, 6, 7}); }
  uint64_t total_rb(); //{ return total(total_bytes, {0, 1, 4, 5}); }
  uint64_t total_wb(); //{ return total(total_bytes, {2, 3, 6, 7}); }
  uint64_t total_crc(); //{ return total(total_bytes, {1, 5}); }
  uint64_t total_crb(); //{ return total(total_bytes, {1, 5}); }
  uint64_t total_wac(); //{ return total(total_bytes, {3, 7}); }
  uint64_t total_wab(); //{ return total(total_bytes, {3, 7}); }
  
  WorkloadStatus status();
  bool printable();
};

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)

}  // namespace ROCKSDB_NAMESPACE
