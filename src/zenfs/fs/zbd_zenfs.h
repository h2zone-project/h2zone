// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include <set>
#include <list>

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

#include "util.h"
#include "zenfs_options.h"
#include "cache_manager.h"

#define LIFETIME_DIFF_NOT_GOOD (100)
#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

namespace ROCKSDB_NAMESPACE {
  
class ZoneFile;
class ZonedBlockDevice;
class Tiering;
class TokenBucket;
class ZenStat;
class HybridBlockDevice;

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime);

struct FileHints;

class Zone {
 private:
  ZonedBlockDevice *zbd_ = nullptr;
  uint64_t start_;
  uint64_t fstart_; /* For fused device */
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  uint64_t fwp_; /* For fused device */
  Env::WriteLifeTimeHint lifetime_;
  HybridBlockDevice* hbd_ = nullptr;
  int level_ = 999;
  FileType file_type_ = kTempFile;
  bool for_wal_ = false;
  bool for_cache_ = false;

  double ts_ = 0.0;
  uint64_t size_compacted_ = 0;
  std::mutex used_mtx_;

  // for posix file system
  std::shared_ptr<FileSystem> fs_ = nullptr;
  std::string posix_path_ = "";
  std::string filename_ = "";
  std::unique_ptr<FSWritableFile> wr_file = nullptr; 
  std::unique_ptr<FSRandomAccessFile> rd_file = nullptr;
  std::atomic<int> wrfd_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, struct zbd_zone *z);
  explicit Zone(ZonedBlockDevice *zbd, uint64_t start,
      uint64_t capacity, uint64_t wp);
  explicit Zone(ZonedBlockDevice* zbd, 
      std::shared_ptr<FileSystem> fs, std::string& path, 
      uint64_t start, uint32_t capacity); 
  ~Zone();

  void PrintCond();
  bool open_for_write_;
  std::atomic<long> used_capacity_;
  std::atomic<int> rand_read_times_;

  std::map<uint64_t, uint64_t> used_sizes_;
  std::map<int, int> levels_;

  IOStatus PhysicalReset();

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  uint64_t capacity() { return capacity_; }
  double util() { return (double)used_capacity_ / max_capacity_; }
  double wp_pct() { return (wp_ >= start_ + max_capacity_) ? 
    100.0 : (double)(wp_ - start_) / max_capacity_ * 100.0; }
  double timestamp() { return ts_; }
  double lifespan();
  void SetLifespan(double lf);
  double rand_read_freq();

  uint64_t start() { return start_; }
  uint64_t fstart() { return fstart_; }
  uint64_t wp() { return wp_; }
  uint64_t fwp() { return fwp_; }
  bool rotational(); 
  bool for_wal() { return for_wal_; }
  bool for_cache() { return for_cache_; }

  uint64_t zone_nr(); 
  bool zoned();
  uint64_t max_capacity() { return max_capacity_; }
  Env::WriteLifeTimeHint lifetime() { return lifetime_; }

  void SetForWal() { for_wal_ = true; }
  void SetForCache() { for_cache_ = true; }
  void ResetForCache() { for_cache_ = false; }
  void AddCompactedBytes(uint64_t bytes) { size_compacted_ += bytes; } 
  void DeductCompactedBytes(uint64_t bytes) { size_compacted_ -= bytes; }
  uint64_t CompactedBytes() { return size_compacted_; }
  bool IsCompacted() { return (size_compacted_ > 0); }
  void SetFstart(uint64_t fstart); 
  void SetFileLifetime(Env::WriteLifeTimeHint lifetime) { 
    lifetime_ = lifetime; 
  }

  IOStatus Append(char *data, uint32_t size, double* latency = nullptr);
  ssize_t PositionedRead(char* data, uint64_t offset, size_t n);

  bool IsUsed() { return (used_capacity_ > 0) || open_for_write_; }
  bool IsFull() { return (capacity_ == 0); }
  bool IsEmpty() { return (wp_ == start_); }

  FileType file_type() { return file_type_; }
  bool is_blob() { return file_type_ == kBlobFile; }
  bool is_sst() { return file_type_ == kTableFile; }
  bool is_wal() { return file_type_ == kWalFile; }

  int level() { return level_; }
  void SetFileType(FileType file_type) { file_type_ = file_type; }
  void SetLevel(int level) { level_ = level; }

  ZonedBlockDevice* zbd() { return zbd_; }
  HybridBlockDevice* hbd() { return hbd_; }
  void SetFzbd(HybridBlockDevice* hbd) { hbd_ = hbd; }

  void CloseWR(); /* Done writing */

  // dbg - not necessary
  void IncFileIdLevel(uint64_t id, int lvl, uint64_t size); 
  void DecFileIdLevel(uint64_t id, int lvl, uint64_t size); 
};

class ZonedBlockDevice {
 public:
  std::string filename_;
  uint32_t block_sz_;
  uint32_t zone_sz_;
  uint32_t zone_cap_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones_;
  std::mutex io_zones_mtx_;
  std::vector<Zone *> meta_zones_;

  int read_f_;
  int read_direct_f_;
  int write_f_;

  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

  bool zoned_ = true;

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  std::condition_variable zone_resources_;
  std::mutex zone_resources_mtx_; /* Protects active/open io zones */

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  // File systems
  std::shared_ptr<FileSystem> fs_ = nullptr;
  std::string posix_file_path_ = "";
  
// public:
  void FindFileSystemPath(std::string); 
  explicit ZonedBlockDevice(std::string bdevname, std::shared_ptr<Logger> logger);
  ZonedBlockDevice() {}
  virtual ~ZonedBlockDevice();

  // Device operations
  IOStatus ZonedDeviceOpen(bool readonly);
  IOStatus BlockDeviceOpen(bool readonly);
  IOStatus FileSystemOpen(bool readonly);

  IOStatus ZonedAddZones(bool readonly);
  IOStatus ConvAddZones();
  IOStatus FileSystemAddZones();
  
  virtual IOStatus CheckScheduler();
  virtual IOStatus ReadRotational();
  virtual IOStatus ReadZoned();

  ssize_t PositionedRead(bool direct, char* scratch, uint64_t offset, 
      size_t n);

  virtual IOStatus Open(bool readonly = false); /* Open() do not have locks */
  void CheckZones();

  virtual Zone *GetIOZone(uint64_t offset);
  virtual Zone *AllocateZone(struct FileHints&);
  Zone *AllocateMetaZone();

  virtual uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

  std::string filename();
  uint32_t GetBlockSize();

  void ResetUnusedIOZones();
  void LogZoneStats();
  virtual void LogZoneUsage();

  int GetReadFD() { return read_f_; }
  int GetReadDirectFD() { return read_direct_f_; }
  int GetWriteFD() { return write_f_; }

  uint64_t zone_sz() { return zone_sz_; }
  uint32_t GetNrZones() { return nr_zones_; }
  std::vector<Zone *> GetMetaZones() { return meta_zones_; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void PutActiveIOZoneToken(); // Decrement active_io_zones_
  void PutOpenIOZoneToken(); // Decrement open_io_zones_

  void IncrementActiveZones(); // Extracted from AllocateZone() 
  void IncrementOpenZones(); // Extracted from AllocateZone() 

  void SetFstart(uint64_t fstart) { fstart_ = fstart; } 
  void SetFend(uint64_t fend) { fend_ = fend; }

  bool rotational() { return rotational_; }
  uint64_t fstart() { return fstart_; }
  uint64_t fend() { return fend_; }

  virtual void SetZenFSOptions(ZenFSOptions& zenfs_options);
  ZenFSOptions zenfs_options();

 protected:
  ZenFSOptions zops_;

 private:
  uint64_t fstart_ = 0;
  uint64_t fend_ = 0;
  bool rotational_ = false;

  std::string ErrorToString(int err);
};

class HybridBlockDevice : public ZonedBlockDevice {
 public:
  explicit HybridBlockDevice(std::vector<std::string> bdevnames, 
      std::shared_ptr<Logger> logger);
  explicit HybridBlockDevice(std::vector<std::string> bdevnames, 
      std::shared_ptr<Logger> logger, 
      FileSystem* fs);
  virtual ~HybridBlockDevice();
  
  void FlushZoneWps();

  virtual IOStatus Open(bool readonly = false) override;
  virtual IOStatus CheckScheduler() override;
  virtual void LogZoneUsage() override;

  virtual Zone *GetIOZone(uint64_t foffset) override;

  /* Reset any unused zones and finish used zones under capacity treshold*/
  Zone* TryResetAndFinish(ZonedBlockDevice* zbd = nullptr);
  Zone* TryAlreadyOpenZone(bool use_rotational, struct FileHints& h, bool& lifetime_not_good);
  IOStatus TryFinishVictim(Zone* finish_victim);

  virtual Zone *AllocateZone(struct FileHints&) override;

  ZonedBlockDevice *GetDevice(uint64_t offset);

  uint32_t GetNrZonedBlockDevices() { return (uint32_t) zbds_.size(); }
  std::vector<uint32_t> GetBlockSizes(); 
  uint32_t GetMaximumBlockSize();
  std::vector<uint32_t> GetZoneSizesInBlk(); 
  std::vector<uint32_t> GetNrsZones(); 

  IOStatus PositionedRead(uint64_t offset, size_t n,
      char* scratch, bool direct, ssize_t* ret, double* latency = nullptr);

  bool CheckOffsetAligned(uint64_t offset);
  std::vector<ZonedBlockDevice*> zbds() { return zbds_; }

  virtual uint64_t GetFreeSpace() override;
  uint64_t GetSsdFreeSpace();
  uint64_t GetSsdFreeSpaceWoWAL();
  uint64_t GetHddFreeSpace();
  uint64_t GetTotalSpace();
  uint64_t GetSsdTotalSpace();
  uint64_t GetSsdTotalSpaceWoWAL();
  uint64_t GetHddTotalSpace();
  int GetSsdIoZone();
  int GetSsdFreeZones();

  std::string fused_filename() { return fused_filename_; }

  bool HasHdd();
  bool HasSsd();
  bool IsHeterogeneous();

  virtual void SetZenFSOptions(ZenFSOptions& zenfs_options) override;

  CacheManager* cache();

  // stat
  void AddStatBytes(bool is_rotational, bool is_read, bool is_compaction_read, 
      bool is_index, int level, uint64_t bytes, double latency = 0.0, 
      int zone_number = 0); 
  void PrintStat();

  Zone* AllocateCacheZone();
  Zone* ReleaseCacheZone();

  void CheckSpanDBMaxLevel();
  void SetZenFS(ZenFS* zenfs);
  int level_m() { return level_m_; }
  uint64_t elapsed_time() { return time(NULL) - start_time_; }
  double elapsed_time_double();
  WorkloadStatus status();

  std::atomic<bool> stop_;

 private:
  bool CheckRotationalTrivial(struct FileHints& fh);
  bool CheckRotationalSpanDB(struct FileHints& fh);
  bool CheckRotationalMutant(struct FileHints& fh);
  bool CheckRotationalH2Zone(struct FileHints& fh);
  bool PreAllocateCheckRotational(struct FileHints& fh);
  int GetSsdEmptyZonesNum();
  void ComputeLevelM();
  int ComparePriority(Zone*, Zone*);
  void PrintSsdUtil();
  void UpdateFileDistributions(bool print = true);
  Zone* CheckAndRollToNewZone(uint64_t length);

  std::string fused_filename_;
  std::queue<Zone *> ssd_cache_zones_;
  std::vector<std::string> filenames_;
  std::vector<ZonedBlockDevice*> zbds_;
  std::vector<uint64_t> global_offsets_; 
  std::shared_ptr<Logger> logger_;

  int level_m_ = 1;
  int num_ssd_zones_ = 0;


  CacheManager* cache_ = nullptr;
  ZenFS* zenfs_ = nullptr;

  std::vector<std::map<uint64_t, Zone*>> fstart_2_io_zone_;

  uint64_t ssd_total_space_ = 0;
  uint64_t hdd_total_space_ = 0;

  std::map<int, int> level_capacities_;
  std::map<int, int> storage_demands_;
  int total_storage_demands_ = 0;
  double last_time_capacity_updated_ = 0.0;

  int num_wal_zones_ = 0;
  ZenStat* stat_;

  // stat

  double start_time_double;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
