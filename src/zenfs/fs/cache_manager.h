#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#define kBufSize (16 * 1024)

//#include "zenfs_options.h"

#include <condition_variable>
#include <mutex>
#include <queue>
#include <list>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"

#include "zenfs_options.h"

namespace ROCKSDB_NAMESPACE {

class Zone;
class HybridBlockDevice;
class ZenFS;

struct SsdCacheEntry;

struct CacheManagerForFile {
  std::map<uint64_t, SsdCacheEntry*> mp;
};

struct SsdCacheEntry {
  CacheManagerForFile* file = nullptr;
  uint64_t old_off = 0;  // The new offset
  uint64_t new_off = 0;  // The new offset
  uint64_t length = 0;
  uint32_t seq_num = 0;
};

struct AsyncEntry {
  uint64_t file_id = 0;
  uint64_t old_off = 0;
  uint64_t length = 0;
  char* content = nullptr;
};

class CacheManager {
 private:
  HybridBlockDevice* hbd_ = nullptr;
  ZenFS* zenfs_ = nullptr;
  std::map<uint64_t, CacheManagerForFile*> file_managers_;
  Zone* zone_ = nullptr;
  char buf_[kBufSize];
  uint64_t buf_foffset_ = 0;
  int buf_ptr_ = 0;
  int seq_num = 0;

  // dbg
  int read_hit = 0;
  int read_total = 0;
  int insert_total = 0;

  std::queue<SsdCacheEntry*> ssd_cache_entries_;
  std::list<AsyncEntry*> async_entries_;
  std::mutex mtx_;
  std::mutex amtx_;

  ZenFSOptions zops_;

  IOStatus BufferedWrite(char* content, uint64_t length, uint64_t* foffset);
  IOStatus BufferedRead(uint64_t foffset, uint64_t length, char* content);
  void GenerateTrailer(char* result, uint64_t length); 

  IOStatus Insert(uint64_t file_id, uint64_t offset, uint64_t length, char* result);

 public:
  CacheManager(HybridBlockDevice* hbd, ZenFSOptions zops);
  ~CacheManager();
  IOStatus AsyncInsert(uint64_t file_id, uint64_t offset, uint64_t length, 
      char* result);
  IOStatus Sync();
  IOStatus ClearFileEntries(uint64_t file_id);
  IOStatus ClearOldestEntries(Zone* z);

  IOStatus Read(uint64_t file_id, uint64_t old_offset, uint64_t& length, char** result);
  void SetZenFS(ZenFS* zenfs);

  // dbg
  void LogStatus();
};

}
#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
