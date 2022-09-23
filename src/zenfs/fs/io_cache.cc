#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "io_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <string>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "util/coding.h"

#include "fs_zenfs.h"
#include "zbd_zenfs.h"

#define unlikely(x)  __builtin_expect((x),0)

namespace ROCKSDB_NAMESPACE {

IOStatus ZoneFile::InformCacheRecords(Cache::KeyRecords& key_records) {
  if (zops_.print_cache) {
    if (key_records.target.size() > 0) {
      fprintf(stderr, "records num %d\n", (int)key_records.target.size()); 
    }
  }

  if (cache_ == nullptr && zops_.enable_cache_manager) {
    cache_ = hbd_->cache();
  }

  IOStatus s;

  for (auto& it : key_records.target) {
    if (it.record == Cache::KeyRecord::kEvictDueToInsert
        && !zops_.enable_no_hinted_caching
        && cache_ != nullptr
        && it.content != nullptr) {
      if (zops_.print_cache) {
        fprintf(stderr, "num %d record offset %d length %d\n",
            it.file_number, it.offset, it.length); 
      }
      cache_->AsyncInsert(it.file_number, it.offset, it.length, it.content); 
    }

    if (zops_.print_cache) {
      fprintf(stderr, "file %ld num %ld Inform num %d off %d "
          "length %d content %s - %s\n", 
          id(), number(), it.file_number, it.offset, it.length, 
          Slice(it.content, 10).ToString(true).c_str(), 
          Slice(it.content + it.length - 10, 10).ToString(true).c_str());
    }

    if (it.content) {
      delete[] it.content;
      it.content = nullptr;
    }
  }

  key_records.target.clear();

  return IOStatus::OK();
}

void ZoneFile::SetCacheManager(CacheManager* cache) {
  cache_ = cache;
}

IOStatus ZoneFile::ReadCachedDataBlock(const IOOptions& opts, uint64_t offset, 
    uint64_t length, bool direct, char** result) {
  if (!zops_.enable_cache_manager) {
    return IOStatus::NoSpace("No cache manager");
  }

  if (cache_ == nullptr) {
    cache_ = hbd_->cache();
    if (cache_ == nullptr) {
      return IOStatus::NoSpace("No cache manager");
    }
  }

  // Not direct read: retrieve a block from the cache
  if (!direct) {
    return cache_->Read(number(), offset, length, result);
  } else {
    // Direct read
    // orig_offset: unaligned offset 
    //      offset: aligned offset
    // orig_length: unaligned length
    //      length: aligned length
    // use the unaligned offset to retrieve a block from the cache

    uint64_t orig_offset = opts.orig_offset;
    uint64_t orig_length = opts.orig_length;

    char* db_result = nullptr;

    if (zops_.print_cache) {
      fprintf(stderr, "direct read: id %ld orig_offset %ld offset %ld length %ld\n", 
          id(), orig_offset, offset, length);
    }

    if (orig_offset >= offset && orig_offset - offset < 4096) {
      uint64_t db_length = 0;

      // db_length include the trailers
      IOStatus s = cache_->Read(number(), orig_offset, db_length, &db_result);

      if (db_result) {
        if (s.ok() && orig_offset + db_length <= offset + length && db_length > 0) {
          if (db_length != orig_length) {
            fprintf(stderr, "orig_length not the same! %ld v.s. %ld\n", 
                db_length, orig_length); 
            return IOStatus::Corruption("length not the same");
          }

          if (zops_.print_cache) {
            fprintf(stderr, "direct read: success, [%ld, -- %ld + %ld -- %ld)\n", 
                offset, orig_offset, db_length, offset + length); 
          }
          
          *result = new char[length];

          // Zero padding to the space 
          memset(*result, 0, length);
          memcpy(*result + (orig_offset - offset), db_result, db_length); 

        } else {
          if (db_length == 0) {
            fprintf(stderr, "Warning: file id %ld, "
                "offset %ld + length %ld, db_length is still zero\n", 
                id(), offset, length);
          } else if (orig_offset + db_length > offset + length) {
            fprintf(stderr, "Warning: file id %ld, "
                "orig %ld + db %ld > offset %ld + length %ld\n", 
                id(), orig_offset, db_length, offset, length);
          }

          if (s.ok()) {
            s = IOStatus::Corruption("cache or read corruption");
          }
        }
        
        delete[] db_result;
      }

      return s;
    }

    return IOStatus::Corruption("Direct read hint lost");
  }
}

IOStatus ZonedRandomAccessFile::InformCacheRecords(
    Cache::KeyRecords& key_records) {
  return zone_file_->InformCacheRecords(key_records); 
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
