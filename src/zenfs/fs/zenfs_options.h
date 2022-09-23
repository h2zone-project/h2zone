#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <cstdio>
#include <cstring>
#include <cinttypes>
#include <string>

namespace ROCKSDB_NAMESPACE {

struct ZenFSOptions {
  // Enable the data placement  
  bool enable_data_placement = false;
  bool enable_explicit_migration = false;

  // Enable SSD caching 
  bool enable_cache_manager = false;
  bool enable_capacity_calibration = true;
  bool enable_aggressive_deletion = false;

  double dynamic_throughput_factor = 1.0;

  // Debug info
  bool print_zraf_read = false;
  bool print_zwf_write = false;
  bool print_cache = false;
  bool print_window = false;
  bool print_migration = false;
  bool log_zwf_write = false;
  bool log_zsf_read = false;
  bool log_zraf_read = false;
  bool log_zf_pushextent = false;
  bool kv_sep = false;

  bool log_zf_read_hdd = false;
  bool log_zf_read = false;

  bool enable_no_hinted_caching = false; // Need enable_cache_manager
  bool enable_trivial_data_placement = false;
  bool enable_spandb_data_placement = false;
  bool enable_mutant_data_placement = false;

  uint64_t constrained_ssd_space = 0; // 150ull * 1024 * 1024 * 1024;
  uint64_t constrained_hdd_space = 0;
  uint64_t max_wal_size = 2ull * 1024 * 1024 * 1024;
  int level_m = 1;
  uint64_t sst_size = 256ull * 1024 * 1024;

  uint64_t ssd_write_bound = 903 * 1024 * 1024;
  uint64_t hdd_write_bound = 210 * 1024 * 1024;
  uint64_t hdd_randread_bound = 57;
  uint64_t explicit_throughput = 4 * 1024 * 1024;
  uint64_t buffer_size_in_block = 256;

  // 0: use factor k to calculate all levels
  // 1: capacity += compacted zones 
  uint64_t level_capacity_impl = 1;

  bool test_cache_correctness = false;
  bool print_posix_rw = false;

  std::string posix_file_path = "";
  std::string posix_device = "";

  uint64_t posix_space_size = 20ull * 1024 * 1024 * 1024;
  bool random_read_test = false;

  ZenFSOptions() {} 
  void Print();
  void Read();
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
