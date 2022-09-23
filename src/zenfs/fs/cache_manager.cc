#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include "zbd_zenfs.h"
#include "io_zenfs.h"
#include "fs_zenfs.h"
#include "cache_manager.h"
#include "util/crc32c.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

CacheManager::CacheManager(HybridBlockDevice* hbd, ZenFSOptions zops) {
  hbd_ = hbd;
  zops_ = zops;
}

CacheManager::~CacheManager() {
  LogStatus();
  for (auto& it : file_managers_) {
    for (auto& it2 : it.second->mp) {
      delete it2.second;
    }
    delete it.second;
  }
}

IOStatus CacheManager::AsyncInsert(uint64_t file_id, uint64_t old_offset, 
    uint64_t length, char* content) {
  AsyncEntry* antry = new AsyncEntry();
  antry->file_id = file_id;
  antry->old_off = old_offset;
  antry->length = length;
  antry->content = new char[length];
  memcpy(antry->content, content, length);

  amtx_.lock();
  async_entries_.push_back(antry);
  amtx_.unlock();

  return IOStatus::OK();
}

IOStatus CacheManager::Sync() {
  amtx_.lock();
  while (!async_entries_.empty()) {
    AsyncEntry* antry = async_entries_.front();
    async_entries_.pop_front();

    Insert(antry->file_id, antry->old_off, antry->length, antry->content);
    delete[] antry->content;
    delete antry;
  }
  amtx_.unlock();

  return IOStatus::OK();
}

// Maintain a buffer. 
// You should not provide the foffset - Because you are appending.
// Assume that mtx is locked
IOStatus CacheManager::BufferedWrite(char* content, uint64_t length, 
    uint64_t* foffset) {
  IOStatus s;
  void* alignbuf;
  int ret;

  if (zone_ == nullptr) {
    return IOStatus::Corruption("Zone should not be empty");
  }

  // We do not split the entry to multiple zones
  if (zone_->capacity() < //(zone_->fwp() - zone_->fstart()) + 
      length + buf_ptr_) 
  {
    // Not enough space in the zone

    if (buf_ptr_) {
      // There are some data to append. Zero-padding for alignment
      ret = posix_memalign(&alignbuf, sysconf(_SC_PAGESIZE), kBufSize);
      if (ret) {
        return IOStatus::IOError("failed allocating alignment "
            "write buffer\n");
      }
      memset(buf_ + buf_ptr_, 0, kBufSize - buf_ptr_);
      memcpy(alignbuf, buf_, kBufSize);

      s = zone_->Append((char*)alignbuf, kBufSize);
      free(alignbuf);

      if (!s.ok()) { 
        fprintf(stderr, "Buffer full append error\n");
        return s;
      }

      if (zops_.print_cache) {
        fprintf(stderr, "Full append: 0x%lx\n", zone_->fwp()); 
      }

      buf_ptr_ = 0;
    }

//    if (zone_->capacity() == 0) {
//      fprintf(stderr, "Cache zone full\n");
//    }

    zone_ = hbd_->AllocateCacheZone();
    ClearOldestEntries(nullptr);
  }

  if (zops_.print_cache) {
    fprintf(stderr, "BufferedWrite(), length %ld\n", length);
  }

  uint64_t buffer_left = kBufSize - buf_ptr_; // Left space in the buffer 
  uint64_t data_left = length;        // Left data to append
  uint64_t data_written = 0;
  *foffset = zone_->fwp() + buf_ptr_;
  ret = posix_memalign(&alignbuf, sysconf(_SC_PAGESIZE), kBufSize);

  while (buffer_left < data_left) {
    if (ret) {
      return IOStatus::IOError("failed allocating alignment "
          "write buffer 2\n");
    }

    memcpy(buf_ + buf_ptr_, content + data_written, 
        (uint32_t)buffer_left);
    memcpy(alignbuf, buf_, kBufSize);
    s = zone_->Append((char*)alignbuf, kBufSize);

    if (!s.ok()) { 
      fprintf(stderr, "Buffer full append 2 error\n");
      return s;
    }

    if (zops_.print_cache) {
      fprintf(stderr, "Full append 2: 0x%lx\n", zone_->fwp()); 
    }

//    if (zone_->capacity() == 0) {
//      fprintf(stderr, "Cache zone full after full append 2\n");
//    }

    buf_ptr_ = 0;
    data_left -= buffer_left;
    data_written += buffer_left;
    buffer_left = kBufSize;
  }

  free(alignbuf);

  memcpy(buf_ + buf_ptr_, content + data_written, (uint32_t)data_left);
  buf_ptr_ += data_left;

  return IOStatus::OK();
}

// Raw data
IOStatus CacheManager::BufferedRead(uint64_t foffset, uint64_t length, 
    char* content) {

  ssize_t ret = 0;

  // Data maybe in the current zone
  if (foffset >= zone_->fstart() &&
      foffset < zone_->fstart() + zone_->max_capacity()) {
    // Written data is less than required data
    if (zone_->fwp() + buf_ptr_ < foffset + length) {
      return IOStatus::Corruption("not saved in zone");
    } 

    // All content in buffer
    if (zone_->fwp() <= foffset) {
      memcpy(content, buf_ + (foffset - zone_->fwp()), length);
      return IOStatus::OK();
    }

    // All content in zone 
    if (foffset + length <= zone_->fwp()) {
      hbd_->PositionedRead(foffset, length, content, false, &ret); 
      if (ret <= 0) {
        return IOStatus::Corruption("Position read failed");
      }
      return IOStatus::OK();
    }

    // Part of content in zone
    uint64_t ssd_left = zone_->fwp() - foffset;
    uint64_t data_left = length - ssd_left;
    hbd_->PositionedRead(foffset, ssd_left, content, false, &ret);
    if (ret <= 0) {
      return IOStatus::Corruption("Position read failed");
    }
    memcpy(content + ssd_left, buf_, data_left);

    return IOStatus::OK();
  }

  hbd_->PositionedRead(foffset, length, content, false, &ret);  
  return IOStatus::OK();
}

// assert mtx is held
IOStatus CacheManager::ClearOldestEntries(Zone* z = nullptr) {
  int cnt = 0, invalid_cnt = 0;
  bool need_lock = (z != nullptr);
  Zone* param_z = z;

  if (z == nullptr) {
    z = zone_;
  } 

  if (need_lock) {
    mtx_.lock();
  }

  if (param_z != nullptr && param_z == zone_) {
    zone_ = nullptr;
  }

  while (!ssd_cache_entries_.empty()) {
    SsdCacheEntry* entry = ssd_cache_entries_.front();

    // If the entry belongs to the next zone
    if (entry->new_off >= z->fstart() && 
        entry->new_off < z->fstart() + z->max_capacity()) {

      if (zops_.print_cache) {
        fprintf(stderr, "ClearOldestEntries entry 0x%lx old_off %ld "
            "seq_num %d file 0x%lx\n", (uint64_t)entry, 
            entry->old_off, entry->seq_num, (uint64_t)entry->file);
      }

      // If the entry is valid
      if (entry->file != nullptr) { 
        CacheManagerForFile* file = entry->file;
        file->mp.erase(entry->old_off);
      } else {
        invalid_cnt++;
      } 

      cnt++;
      ssd_cache_entries_.pop();
      delete entry;
    } else {
      break;
    }
  }

  if (need_lock) {
    mtx_.unlock();
  }

  return IOStatus::OK();
}

IOStatus CacheManager::ClearFileEntries(uint64_t file_id) {
  CacheManagerForFile* file = nullptr;

  mtx_.lock();
  auto it = file_managers_.find(file_id);
  if (it != file_managers_.end()) {
    file = it->second;
  }

  if (file != nullptr) {

    if (zops_.print_cache) {
      fprintf(stderr, "ClearFileEntries %d clear %d entries 0x%lx\n", 
          (int)file_id, (int)file->mp.size(), (uint64_t)file);
    }

    // Invalidate all entries of the file
    for (auto& it0 : file->mp) {
      if (zops_.print_cache) {
        fprintf(stderr, "ClearFileEntries %d clear entry 0x%lx"
            " with file 0x%lx\n", 
            (int)file_id, (uint64_t)it0.second, (uint64_t)it0.second->file);
      }

      it0.second->file = nullptr;
    }

    delete file;
    file_managers_.erase(file_id);
  }

  mtx_.unlock();

  return IOStatus::OK();
}

IOStatus CacheManager::Insert(
    uint64_t file_id, uint64_t offset, uint64_t length, 
    char* content) {
  // Initialize

  mtx_.lock();
  if (zops_.print_cache) {
    fprintf(stderr, "Insert() %lu offset %lu size %ld\n",
        file_id, offset, length);
  }

  {
    auto it = file_managers_.find(file_id);
    if (it != file_managers_.end()) {
      CacheManagerForFile* f = it->second;

      auto it2 = f->mp.find(offset);
      if (it2 != f->mp.end()) {
        // Already have the entry. Do not save again
        mtx_.unlock();
        return IOStatus::OK(); 
      }
    }
  }

  bool rotational = false;
  IOStatus s = zenfs_->CheckOffsetRotational(file_id, offset, &rotational);
  if (!s.ok() || !rotational) {
    mtx_.unlock();
    if (zops_.print_cache) {
      fprintf(stderr, "Insert() not included s %s rot %d\n",
          s.ToString().c_str(), (int)rotational);
    }
    return IOStatus::OK();
  }

  if (zone_ == nullptr) {  
    zone_ = hbd_->AllocateCacheZone();
  }

  if (zone_ == nullptr) {
    mtx_.unlock();
    return IOStatus::NoSpace("No cache space");
  }

  uint64_t foffset;
  s = BufferedWrite(content, length, &foffset);
  if (!s.ok()) {
    fprintf(stderr, "Insert() failed, because of "
        "write error: %s\n", s.ToString().c_str());
    mtx_.unlock();
    return s;
  }

  if (zops_.print_cache) {
    fprintf(stderr, "Insert() fid %ld "
        "offset %ld length %ld foffset 0x%lx\n",
        file_id, offset, length, foffset);
  }

  insert_total++;
  SsdCacheEntry* entry = new SsdCacheEntry();
  CacheManagerForFile* fm = nullptr;

  if (!file_managers_.count(file_id)) {
    fm = new CacheManagerForFile();
    file_managers_[file_id] = fm;
  } else {
    fm = file_managers_[file_id];
  }
  entry->file = fm;
  entry->old_off = offset;
  entry->new_off = foffset;
  entry->length = length;
  entry->seq_num = seq_num++;
  ssd_cache_entries_.push(entry);
  fm->mp[offset] = entry;

  if (zops_.print_cache) {
    fprintf(stderr, "Insert fm 0x%lx entry 0x%lx old_off %ld"
        " seq_num %d file 0x%lx\n", (uint64_t)fm, (uint64_t)entry, 
        entry->old_off, entry->seq_num, (uint64_t)entry->file);
  }

  mtx_.unlock();
  return IOStatus::OK();
}

void CacheManager::LogStatus() {
  mtx_.lock();
  fprintf(stderr, "file_managers\n");
  for (auto& it : file_managers_) {
    fprintf(stderr, "fn %ld 0x%lx\n", 
        it.first, (uint64_t)it.second);
    for (auto& it2 : it.second->mp) {
      fprintf(stderr, "%ld -> 0x%lx %ld 0x%lx %ld %d\n", 
          it2.first, (uint64_t)it2.second->file, 
          it2.second->old_off, it2.second->new_off,
          it2.second->length, it2.second->seq_num);
    }
  }
  mtx_.unlock();
}

// Real length, including the trailer
IOStatus CacheManager::Read(uint64_t file_id, uint64_t old_offset, 
    uint64_t& length, char** result) {
  read_total++;

  if (read_total % 10000 == 0) {
    fprintf(stderr, "(inside, not accurate) read_hit "
        "%d read_total %d insert_total %d\n", read_hit, read_total, insert_total);
  }

  // From v1.0.9: Read from async entries first 
  {
    amtx_.lock();
    for (auto it = async_entries_.begin(); it != async_entries_.end(); ++it) {
      AsyncEntry* antry = *it;
      if (antry->file_id == file_id && antry->old_off == old_offset &&
          (antry->length == length - 5 || length == 0)) {
        *result = new char[antry->length + 5];
        memcpy(*result, antry->content, antry->length);

        if (length == 0) {
          length = antry->length + 5;
        }

        read_hit++;
        GenerateTrailer(*result, antry->length);

        amtx_.unlock();
        return IOStatus::OK();
      }
    }
    amtx_.unlock();
  }

  mtx_.lock();
  // Read from buffer or from SSD 
  if (!file_managers_.count(file_id)) {
    mtx_.unlock();
    return IOStatus::NoSpace("No cache entry"); 
  }

  CacheManagerForFile* f = file_managers_[file_id];
  IOStatus s;
  SsdCacheEntry* entry = nullptr;

  auto it = f->mp.find(old_offset);
  if (it != f->mp.end()) {
    entry = it->second;
  }

  if (entry != nullptr) {
    uint64_t new_foffset = entry->new_off;
    uint64_t new_length = entry->length;

    if (!zops_.enable_no_hinted_caching) {
      if (length - 5 != new_length && length != 0) {
        mtx_.unlock();
        return IOStatus::Corruption("Length different");
      }

      if (length == 0) {
        length = new_length + 5;
      }

      *result = new char[length];
      s = BufferedRead(new_foffset, length - 5, *result);

      if (!s.ok()) {
        mtx_.unlock();
        return s;
      }

      read_hit++;

      GenerateTrailer(*result, length - 5);
    } else {
      // hinted caching
      if (length != new_length) {
        mtx_.unlock();
        return IOStatus::Corruption("Length different");
      }

      *result = new char[length];
      s = BufferedRead(new_foffset, length, *result);
      if (!s.ok()) {
        mtx_.unlock();
        return s;
      }

      read_hit++;
    }
  } else {
    mtx_.unlock();
    return IOStatus::NoSpace("No cache entry"); 
  }

  mtx_.unlock();
  return IOStatus::OK();
}

// length do not include trailer
void CacheManager::GenerateTrailer(char* result, uint64_t length) {
  char trailer[5];
  trailer[0] = 0;  // TODO should follow the compression type - ignore here
  uint32_t crc = crc32c::Value(result, length);
  crc = crc32c::Extend(crc, trailer, 1);
  uint32_t checksum = crc32c::Mask(crc);
  
  EncodeFixed32(trailer + 1, checksum);
  memcpy(result + length, trailer, 5);
}

void CacheManager::SetZenFS(ZenFS* zenfs) {
  zenfs_ = zenfs;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
