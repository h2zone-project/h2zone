// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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

Status ZoneExtent::DecodeFrom(Slice* input) {
  if (input->size() != (sizeof(fstart_) + sizeof(length_)))
    return Status::Corruption("ZoneExtent", "Error: length missmatch");

  GetFixed64(input, &fstart_);
  GetFixed32(input, &length_);
  return Status::OK();
}

void ZoneExtent::EncodeTo(std::string* output) {
  PutFixed64(output, fstart_);
  PutFixed32(output, length_);
}

enum ZoneFileTag : uint32_t {
  kFileID = 1,
  kFileName = 2,
  kFileSize = 3,
  kWriteLifeTimeHint = 4,
  kExtent = 5,
  kLevel = 6,
  kFileType = 7,
};

void ZoneFile::EncodeTo(std::string* output, uint32_t extent_start) {
  PutFixed32(output, kFileID);
  PutFixed64(output, id());

  PutFixed32(output, kFileName);
  PutLengthPrefixedSlice(output, Slice(filename_));

  PutFixed32(output, kFileSize);
  PutFixed64(output, fileSize);

  PutFixed32(output, kWriteLifeTimeHint);
  PutFixed32(output, (uint32_t)fh_.lifetime);

  PutFixed32(output, kLevel);
  PutFixed32(output, (uint32_t)fh_.level); 

  PutFixed32(output, kFileType);
  PutFixed32(output, (uint32_t)fh_.file_type); 

  for (uint32_t i = extent_start; i < extents_.size(); i++) {
    std::string extent_str;

    PutFixed32(output, kExtent);
    extents_[i]->EncodeTo(&extent_str);
    PutLengthPrefixedSlice(output, Slice(extent_str));
  }

  /* We're not encoding active zone and extent start
   * as files will always be read-only after mount */
}

Status ZoneFile::DecodeFrom(Slice* input) {
  uint32_t tag = 0;
  uint32_t lt;

  GetFixed32(input, &tag);
  if (tag != kFileID || !GetFixed64(input, &file_id_))
    return Status::Corruption("ZoneFile", "File ID missing");

  while (true) {
    Slice slice;
    ZoneExtent* extent;
    Status s;

    if (!GetFixed32(input, &tag)) break;
//    Info(logger_, "ZoneFile::DecodeFrom() tag = %d (%d %d %d %d)\n", tag,
//	kFileName, kFileSize, kWriteLifeTimeHint, kExtent);

    switch (tag) {
      case kFileName:
        if (!GetLengthPrefixedSlice(input, &slice))
          return Status::Corruption("ZoneFile", "Filename missing");
        filename_ = slice.ToString();
        if (filename_.length() == 0)
          return Status::Corruption("ZoneFile", "Zero length filename");
	Info(logger_, "ZoneFile::DecodeFrom() %s\n", filename_.c_str());
        break;
      case kFileSize:
        if (!GetFixed64(input, &fileSize))
          return Status::Corruption("ZoneFile", "Missing file size");
        break;
      case kWriteLifeTimeHint:
        if (!GetFixed32(input, &lt))
          return Status::Corruption("ZoneFile", "Missing life time hint");
        fh_.lifetime = (Env::WriteLifeTimeHint)lt;
        break;
      case kLevel:
        if (!GetFixed32(input, &lt))
          return Status::Corruption("ZoneFile", "Missing level hint");
        fh_.level = (int)lt;
        break;
      case kFileType:
        if (!GetFixed32(input, &lt))
          return Status::Corruption("ZoneFile", "Missing file type hint");
        break;
      case kExtent:
        extent = new ZoneExtent(0, 0, nullptr);
        GetLengthPrefixedSlice(input, &slice);
        s = extent->DecodeFrom(&slice);
        if (!s.ok()) {
          delete extent;
          return s;
        }
        extent->SetZone(hbd_->GetIOZone(extent->fstart()));
        if (!extent->zone()) {
	  Info(logger_, "fstart(): %lu \n", extent->fstart());
          return Status::Corruption("ZoneFile", "Invalid zone extent");
	}
        extent->zone()->used_capacity_ += extent->length();
        extent->zone()->IncFileIdLevel(id(), level(), extent->length());
	extent->zone()->SetLevel(level());
	extent->zone()->SetFileType(file_type());
        if (extent->zone()->rotational()) {
          hdd_size_ += extent->length();
        } else {
          ssd_size_ += extent->length();
        }
        extents_.push_back(extent);
        break;
      default:
        return Status::Corruption("ZoneFile", "Unexpected tag");
    }
  }

  SanitizeHints();
  MetadataSynced();
  return Status::OK();
}

Status ZoneFile::MergeUpdate(ZoneFile* update) {
  if (id() != update->id())
    return Status::Corruption("ZoneFile update", "ID missmatch");

  Rename(update->filename());
  SetFileSize(update->file_size());
  SetWriteLifeTimeHint(update->GetWriteLifeTimeHint());

  std::vector<ZoneExtent*> update_extents = update->GetExtents();

  std::map<Zone*, uint64_t> zones2sz;

  for (long unsigned int i = 0; i < update_extents.size(); i++) {
    ZoneExtent* extent = update_extents[i];
    Zone* zone = extent->zone();
    zone->used_capacity_ += extent->length();
    if (zone->rotational()) {
      hdd_size_ += extent->length();
    } else {
      ssd_size_ += extent->length();
    }
    extents_.push_back(new ZoneExtent(extent->fstart(), extent->length(), zone));

    zones2sz[zone] += extent->length();
  }

  // dbg for looking at the levels
  for (auto it : zones2sz) {
    it.first->IncFileIdLevel(id(), level(), it.second);
  }

  MetadataSynced();

  return Status::OK();
}

ZoneFile::ZoneFile(HybridBlockDevice* hbd, std::string filename,
    const FileOptions& file_opts, uint64_t file_id, std::shared_ptr<Logger>& logger)
    : hbd_(hbd),
      file_opts_(file_opts),
      active_zone_(NULL),
      extent_fstart_(0),
      extent_filepos_(0),
      fileSize(0),
      filename_(filename),
      file_id_(file_id),
      nr_synced_extents_(0),
      logger_(logger) {

  fh_.level = file_opts.level;
  create_time_ = hbd_->elapsed_time_double();
  compaction_hints_.start_level = 0;
  compaction_hints_.output_level = 0;
  compaction_hints_.is_input = false;
  compaction_hints_.is_involved = false;
  compaction_hints_.job_id = 0;

  rand_read_times_ = 0;

  SanitizeHints();
}

double ZoneFile::lifespan() { 
  return hbd_->elapsed_time_double() - create_time_;
}

std::string ZoneFile::filename() { return filename_; }

std::string ZoneFile::sfilename() {
  int pos = static_cast<int>(filename_.find_last_of('/'));
  return filename_.substr(pos + 1, filename_.length() - pos - 1);
}

void ZoneFile::Rename(std::string name) { 
  filename_ = name; 

  // Update the file type
  SanitizeHints();
}

uint64_t ZoneFile::file_size() { return fileSize; }

void ZoneFile::SetFileSize(uint64_t sz) { fileSize = sz; }

ZoneFile::~ZoneFile() {
  SetDeleted();
  if (fh_.is_migrated) {
    fprintf(stderr, "The deleted file is being migrated. Wait\n");
  }

  while (fh_.is_migrated) {
    // Wait for the migration to complete
    usleep(10000);
  }

  delete_time_ = hbd_->elapsed_time_double(); 

  ReleaseExtents();

  if (cache_ == nullptr && zops_.enable_cache_manager) {
    cache_ = hbd_->cache();
  }

  if (cache_ != nullptr) {
    cache_->ClearFileEntries(number());
  }

  CloseWR();
}

void ZoneFile::CloseWR() {
  ssd_size_ = 0, hdd_size_ = 0;
  rotational(true);  // Calculate ssd size and hdd size

  if (active_zone_) {
    active_zone_->CloseWR();

    active_zone_ = NULL;
    Info(logger_, "Close write: lvl %d id %ld num %ld type %d name %s extents %d "
      "size %.3lf KiB media %s (ssd %.1lf %% hdd %.1lf %%)\n", 
      level(), id(), number(), file_type(), sfilename().c_str(), 
      (int)extents_.size(), (double)file_size() / 1024.0, 
      rotational(true) ? "hdd" : "ssd", 
      (double)ssd_size_ / (ssd_size_ + hdd_size_ + 1) * 100.0, 
      (double)hdd_size_ / (ssd_size_ + hdd_size_ + 1) * 100.0);
  }

  close_time_ = hbd_->elapsed_time_double();
  open_for_wr_ = false;
}

void ZoneFile::OpenWR() {
  open_for_wr_ = true;
}

bool ZoneFile::IsOpenForWR() {
  return open_for_wr_;
}

// Assume extents_mtx_ locked.
// The dev_offset is still the mapped offset
ZoneExtent* ZoneFile::GetExtent(uint64_t file_offset, uint64_t* dev_offset) {
  ZoneExtent* ret = nullptr;
  uint64_t real_file_offset = file_offset;

  for (unsigned int i = 0; i < extents_.size(); i++) {
    if (file_offset < extents_[i]->length()) {  // Reach the object extent
      *dev_offset = extents_[i]->fstart() + file_offset;
      ret = extents_[i];
      break;
    } else {
      file_offset -= extents_[i]->length();
    }
  }

  // migrated extents may also contain data
  if (ret && ret->zone()->rotational() && fh_.is_migrated) {
    file_offset = real_file_offset;

    for (unsigned int i = 0; i < new_extents_.size(); i++) {
      if (file_offset < new_extents_[i]->length()) {
        *dev_offset = new_extents_[i]->fstart() + file_offset;
        if (!new_extents_[i]->zone()->rotational()) {
          return new_extents_[i];
        }
      } else {
        file_offset -= new_extents_[i]->length();
      }
    }

    if (migrated_extent_ && file_offset < migrated_extent_->length()) {
      *dev_offset = migrated_extent_->fstart() + file_offset;
      return migrated_extent_;
    }
  }

  return ret;
}

// TODO Optimize: Consider the extents during migration.
IOStatus ZoneFile::PositionedRead(uint64_t offset, size_t n, const IOOptions& opts, 
    Slice* result, char* scratch, bool direct) {

  char* ptr;
  uint64_t r_off;
  size_t r_sz;
  ssize_t r = 0;
  size_t read = 0;
  ZoneExtent* extent;
  uint64_t extent_end;
  IOStatus s;
  char* read_cached = nullptr;

  if (offset >= fileSize) {
    *result = Slice(scratch, 0);
    return IOStatus::OK();
  }

  r_off = 0;
  extents_mtx_.lock();
  /* Use the file offset the get the extent */
  extent = GetExtent(offset, &r_off);  

  if (!extent) {
    /* read start beyond end of (synced) file data*/
    *result = Slice(scratch, 0);
    extents_mtx_.unlock();
    return s;
  }

  extent->IncRef();
  extents_mtx_.unlock();
  extent_end = extent->fstart() + extent->length();

  if (is_sst() && pattern_ != FSAccessPattern::kSequential && 
      zops_.enable_cache_manager && extent->zone()->rotational()) {
    IOStatus s2;
    s2 = ReadCachedDataBlock(opts, offset, n, direct, &read_cached);

    if (s2.ok() && read_cached != nullptr && 
        !zops_.test_cache_correctness) {
      memcpy(scratch, read_cached, n);
      *result = Slice((char*)scratch, n);
      delete[] read_cached;
      extent->DecRef();
      return IOStatus::OK();
    }
  }

  /* Limit read size to end of file */
  if ((offset + n) > fileSize)
    r_sz = fileSize - offset;
  else
    r_sz = n;

  ptr = scratch;

  double latency = 0.0;
  int failed_times = 0;

  while (read != r_sz && failed_times < 100) {
    size_t pread_sz = r_sz - read;

    if ((pread_sz + r_off) > extent_end) pread_sz = extent_end - r_off;

    hbd_->PositionedRead(r_off, pread_sz, ptr, direct, &r, &latency);

    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        failed_times++;
        continue;
      }
      break;
    }

    pread_sz = (size_t)r;

    ptr += pread_sz;
    read += pread_sz;
    r_off += pread_sz;

    hbd_->AddStatBytes(extent->zone()->rotational(), true,
        (pattern_ == FSAccessPattern::kSequential), 
        (opts.type == IOType::kIndex), 
        fh_.level, pread_sz, latency, 
        extent->zone()->zone_nr());
    
    if (pattern_ != FSAccessPattern::kSequential) {
      rand_read_times_++;
    }

    if (pattern_ == FSAccessPattern::kSequential) {
      compaction_process_ = (compaction_process_ < offset + n) ?
        offset + n : compaction_process_;
    }

    extent->DecRef();

    /* Read not finished yet, need another extent */
    if (read != r_sz && r_off == extent_end) {
      extents_mtx_.lock();
      extent = GetExtent(offset + read, &r_off);
      if (!extent) {
        /* read beyond end of (synced) file data */
        extents_mtx_.unlock();
        break;
      }

      extent->IncRef();
      extents_mtx_.unlock();

      // Update zbd information
      extent_end = extent->fstart() + extent->length();
      assert(hbd_->CheckOffsetAligned(offset));
    }
  }

  if (read != r_sz) {
    return IOStatus::IOError("read failed\n");
  } 

  if (r < 0) {
    s = IOStatus::IOError("pread error\n");
    read = 0;
  }

  *result = Slice((char*)scratch, read);

  if (zops_.test_cache_correctness && 
      read_cached != nullptr) {
    char *c1 = read_cached;
    char *c2 = scratch;
    size_t sz = n;

    if (direct) {
      c1 = read_cached + (opts.orig_offset - offset);
      c2 = scratch + (opts.orig_offset - offset);
      sz = opts.orig_length;
    }

    int ret = memcmp(c1, c2, sz);

    if (ret != 0) {
      fprintf(stderr, "test cache: %d (n %d, sz %d)\n", 
          ret, (int)n, (int)sz);
      int p = 0;
      for (int i = 0; i < (int)sz; i++) {
        if (c1[i] != c2[i]) {
          fprintf(stderr, "differ from %d\n", i);
          p = i;
          break;
        }
      }

      fprintf(stderr, "c1: %s\n", 
          Slice(c1 + p, (sz-p > 10) ? 10 : sz-p).ToString(true).c_str());
      fprintf(stderr, "c2: %s\n", 
          Slice(c2 + p, (sz-p > 10) ? 10 : sz-p).ToString(true).c_str());
    }

    delete[] read_cached;
  }

  if (zops_.enable_cache_manager && zops_.enable_no_hinted_caching && 
      s.ok() && extent->zone()->rotational()) {
    if (cache_ == nullptr) {
      cache_ = hbd_->cache();
    }
    cache_->AsyncInsert(number(), offset, r_sz, scratch); 
  }

  return s;
}

void ZoneFile::PushExtent() {
  uint64_t length;

  assert(fileSize >= extent_filepos_);

  if (!active_zone_) return;

  if (fh_.is_migrated) {
    length = migrated_size_ - extent_filepos_;
    if (length == 0 || migrated_extent_ == nullptr) {
      if (migrated_extent_) {
        delete migrated_extent_;
      }
      return;
    }
    assert(length <= (active_zone_->fwp() - extent_fstart_));

    new_extents_.push_back(migrated_extent_);
    migrated_extent_ = nullptr;

    active_zone_->used_capacity_ += length;
    active_zone_->IncFileIdLevel(id(), level(), length);

    extent_fstart_ = active_zone_->fwp();
    extent_filepos_ = migrated_size_;
  } else {
    length = fileSize - extent_filepos_;
    if (length == 0) return;

    assert(length <= (active_zone_->fwp() - extent_fstart_));

    extents_.push_back(
        new ZoneExtent(extent_fstart_, length, active_zone_));

    active_zone_->used_capacity_ += length;
    active_zone_->IncFileIdLevel(id(), level(), length);

    extent_fstart_ = active_zone_->fwp();
    extent_filepos_ = fileSize;
  }
}

void ZoneFile::ReleaseExtents(bool original) {
  std::vector<ZoneExtent*>& extents = original ? extents_ : new_extents_;
  for (auto e = std::begin(extents); e != std::end(extents); ++e) {
    while ((*e)->ref > 0) {
      usleep(5000);
    }

    Zone* zone = (*e)->zone();
    assert(zone && zone->used_capacity_ >= (*e)->length());
    zone->used_capacity_ -= (*e)->length();
    if (pattern_ == FSAccessPattern::kSequential) {
      zone->DeductCompactedBytes((*e)->length());
    }
    zone->DecFileIdLevel(id(), level(), (*e)->length()); // dbg
    delete *e;
  }

  extents.clear();

  if (!original && migrated_extent_) {
    while (migrated_extent_->ref.load() > 0) {
      usleep(5000);
    }
    delete migrated_extent_;
    migrated_extent_ = nullptr;
  }
}

/* Assumes that data and size are block aligned */
IOStatus ZoneFile::Append(void* data, int data_size, int valid_size, 
    const IOOptions& opts) {

  if (zops_.print_zwf_write) {
    Info(logger_, "ZF Append(), %s, file_id %lu num %ld valid_size = %d\n", 
        sfilename().c_str(), id(), number_, valid_size);
  }

  uint32_t left = data_size;
  uint32_t wr_size, offset = 0;
  IOStatus s;

  double latency;

  if (active_zone_ == NULL) {
    active_zone_ = hbd_->AllocateZone(fh_); 

    if (!active_zone_) {
      return IOStatus::NoSpace("Zone allocation failure 1\n");
    }

    extent_fstart_ = active_zone_->fwp();
    extent_filepos_ = fileSize;
  }

  while (left) {
    if (active_zone_->capacity() == 0) {
      PushExtent();

      active_zone_->CloseWR();
      active_zone_ = hbd_->AllocateZone(fh_); 

      if (!active_zone_) {
        return IOStatus::NoSpace("Zone allocation failure 2\n");
      }

      extent_fstart_ = active_zone_->fwp();
      extent_filepos_ = fileSize;
    }

    if (active_zone_->capacity() == 0) {
      fprintf(stderr, "[ERROR] capacity is still zero\n");
    }

    wr_size = left;
    if (wr_size > active_zone_->capacity()) wr_size = active_zone_->capacity();

    // change both wp_ and fwp()
    s = active_zone_->Append((char*)data + offset, wr_size, &latency); 

    if (!s.ok()) {
      Debug(logger_, "ZoneFile Append failed status %s\n", s.ToString().c_str());
      return s;
    }

    fileSize += wr_size;
    left -= wr_size;
    offset += wr_size;
    hbd_->AddStatBytes(active_zone_->rotational(), false, false, 
        (opts.type == IOType::kIndex), fh_.level, wr_size, latency, 
        active_zone_->zone_nr());
  }

  fileSize -= (data_size - valid_size);
  return IOStatus::OK();
}

void ZoneFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime) {
  fh_.lifetime = lifetime;
}

void ZoneFile::SetFileType(FileType file_type) {
  fh_.file_type = file_type;
}

void ZoneFile::SetLevel(int level) {
  fh_.level = level;
}

void ZoneFile::SetZenFS(ZenFS* zenfs) {
  zenfs_ = zenfs;
}

bool ZoneFile::rotational(bool closed) {
  if (!open_for_wr_ || closed) {
    if (hdd_size_ == 0 && ssd_size_ == 0) {
      for (auto& it : extents_) {
        if (it->zone()->rotational()) {
          hdd_size_ += it->length();
        } else {
          ssd_size_ += it->length();
        }
      }
    }
    return (hdd_size_ > ssd_size_);
  }
  if (extents_.size() > 0) {
    return extents_[0]->zone()->rotational();
  }
  if (active_zone_) {
    return active_zone_->rotational();
  }

  // Unknown
  return true;
}

// Now use the maximum block size of all devices
ZonedWritableFile::ZonedWritableFile(HybridBlockDevice* hbd, 
                                     const FileOptions& file_opts,
                                     ZoneFile* zone_file,
                                     MetadataWriter* metadata_writer) {
  wp = zone_file->file_size();
  file_opts_ = file_opts;
  assert(wp == 0);

  buffered = !file_opts.use_direct_writes;

  block_sz = hbd->GetMaximumBlockSize();
  buffer_sz = block_sz * zone_file->zops_.buffer_size_in_block;
  buffer_pos = 0;

  zone_file_ = zone_file;

  if (buffered) {
    int ret = posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), buffer_sz);

    if (ret) buffer = nullptr;

    assert(buffer != nullptr);
  }

  metadata_writer_ = metadata_writer;
  zone_file_->OpenWR();
}

ZonedWritableFile::~ZonedWritableFile() {
  zone_file_->CloseWR();
  if (buffered) free(buffer);
};

ZonedWritableFile::MetadataWriter::~MetadataWriter() {}

IOStatus ZonedWritableFile::Truncate(uint64_t size,
                                     const IOOptions& /*options*/,
                                     IODebugContext* /*dbg*/) {
  zone_file_->SetFileSize(size);
  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Fsync(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  IOStatus s;

  buffer_mtx_.lock();
  s = FlushBuffer();
  buffer_mtx_.unlock();
  if (!s.ok()) {
    return s;
  }
  zone_file_->PushExtent();

  s = metadata_writer_->Persist(zone_file_);

  if (!s.ok()) {
    fprintf(stderr, "ZWF::Fsync status %s\n", s.ToString().c_str());
  }

  return s;
}

IOStatus ZonedWritableFile::Sync(const IOOptions& options,
                                 IODebugContext* dbg) {
  return Fsync(options, dbg);
}

IOStatus ZonedWritableFile::Flush(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus ZonedWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                      const IOOptions& options,
                                      IODebugContext* dbg) {
  if (wp < offset + nbytes) return Fsync(options, dbg);

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Close(const IOOptions& options,
                                  IODebugContext* dbg) {
  Fsync(options, dbg);
  zone_file_->CloseWR();

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::FlushBuffer() {
  uint32_t align, pad_sz = 0, wr_sz;
  IOStatus s;
  IOOptions opts;

  if (!buffer_pos) return IOStatus::OK();

  align = buffer_pos % block_sz;
  if (align) pad_sz = block_sz - align;

  if (pad_sz) memset((char*)buffer + buffer_pos, 0x0, pad_sz);

  wr_sz = buffer_pos + pad_sz;

  s = zone_file_->Append((char*)buffer, wr_sz, buffer_pos, opts);
  if (!s.ok()) {
    fprintf(stderr, "ZWF::FlushBuffer status %s\n", s.ToString().c_str());
    fprintf(stderr, "Now zf is open for wr: %d\n", 
        zone_file_->IsOpenForWR());
    return s;
  }

  wp += buffer_pos;
  buffer_pos = 0;

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::BufferedWrite(const Slice& slice, 
    const IOOptions& opts) {

  uint32_t buffer_left = buffer_sz - buffer_pos;
  uint32_t data_left = slice.size();
  char* data = (char*)slice.data();
  uint32_t tobuffer;
  int blocks, aligned_sz;
  int ret;
  void* alignbuf;
  IOStatus s;

  if (buffer_pos || data_left <= buffer_left) {
    if (data_left < buffer_left) {
      tobuffer = data_left;
    } else {
      tobuffer = buffer_left;
    }

    memcpy(buffer + buffer_pos, data, tobuffer);
    buffer_pos += tobuffer;
    data_left -= tobuffer;

    if (!data_left) return IOStatus::OK();

    data += tobuffer;
  }

  if (buffer_pos == buffer_sz) {
    s = FlushBuffer();
    if (!s.ok()) return s;
  }

  if (data_left >= buffer_sz) {
    blocks = data_left / block_sz;
    aligned_sz = block_sz * blocks;

    ret = posix_memalign(&alignbuf, sysconf(_SC_PAGESIZE), aligned_sz);
    if (ret) {
      return IOStatus::IOError("failed allocating alignment write buffer\n");
    }

    memcpy(alignbuf, data, aligned_sz);

    s = zone_file_->Append(alignbuf, aligned_sz, aligned_sz, opts);
    free(alignbuf);

    if (!s.ok()) return s;

    wp += aligned_sz;
    data_left -= aligned_sz;
    data += aligned_sz;
  }

  if (data_left) {
    memcpy(buffer, data, data_left);
    buffer_pos = data_left;
  }

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Append(const Slice& data,
                                   const IOOptions& opts,
                                   IODebugContext* /*dbg*/) {
  IOStatus s;

  if (zone_file_->zops_.print_zwf_write) {
    fprintf(stderr, "ZWF Append %s size %d\n", 
        zone_file_->sfilename().c_str(), (int)data.size());
  }

  if (buffered) {
    buffer_mtx_.lock();
    s = BufferedWrite(data, opts);
    buffer_mtx_.unlock();
  } else {
    s = zone_file_->Append((void*)data.data(), data.size(), data.size(), opts);
    if (s.ok()) wp += data.size();
  }

  if (!s.ok()) {
    fprintf(stderr, "ZWF Append status %s\n", s.ToString().c_str());
    fprintf(stderr, "Now zf is open for wr: %d\n", 
        zone_file_->IsOpenForWR());
  }

  return s;
}

IOStatus ZonedWritableFile::PositionedAppend(const Slice& data, uint64_t offset,
                                             const IOOptions& opts,
                                             IODebugContext* /*dbg*/) {
  IOStatus s;

  if (zone_file_->zops_.print_zwf_write) {
    fprintf(stderr, "ZWF PositionedAppend %s size %d\n", 
        zone_file_->sfilename().c_str(), (int)data.size());
  }

  if (offset != wp) {
    assert(false);
    return IOStatus::IOError("positioned append not at write pointer");
  }

  if (buffered) {
    buffer_mtx_.lock();
    s = BufferedWrite(data, opts);
    buffer_mtx_.unlock();
  } else {
    s = zone_file_->Append((void*)data.data(), data.size(), data.size(), opts);
    if (s.ok()) wp += data.size();
  }

  return s;
}

void ZonedWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
  zone_file_->SetWriteLifeTimeHint(hint);
}

IOStatus ZonedSequentialFile::Read(size_t n, const IOOptions& options,
                                   Slice* result, char* scratch,
                                   IODebugContext* /*dbg*/) {
  IOStatus s;

  s = zone_file_->PositionedRead(rp, n, options, result, scratch, direct_);
  if (s.ok()) rp += result->size();

  return s;
}

IOStatus ZonedSequentialFile::Skip(uint64_t n) {
  if (rp + n >= zone_file_->file_size())
    return IOStatus::InvalidArgument("Skip beyond end of file");
  rp += n;
  return IOStatus::OK();
}

IOStatus ZonedSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                             const IOOptions& options,
                                             Slice* result, char* scratch,
                                             IODebugContext* /*dbg*/) {
  return zone_file_->PositionedRead(offset, n, options, result, scratch, direct_);
}

IOStatus ZonedRandomAccessFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& options,
                                     Slice* result, char* scratch,
                                     IODebugContext* /*dbg*/) const {
  if (zone_file_->zops_.log_zraf_read) {
    Info(zone_file_->logger_, "ZRAF Read() %s offset"
        " %lu size %ld level %d MiB IOtype %d\n", 
        zone_file_->sfilename().c_str(), offset, n, 
        (int)zone_file_->level(), (int)options.type);
  }

  IOStatus s = zone_file_-> PositionedRead(offset, n, options, 
      result, scratch, direct_);
  if (zone_file_->zops_.print_zraf_read && s.ok()) {
    Slice slice(scratch, 20);
    fprintf(stderr, "ZRAF Read() %s fid %ld num %ld offset %lu size %ld content %s\n",
        zone_file_->sfilename().c_str(), 
        zone_file_->id(), zone_file_->number(), offset, n, 
        slice.ToString(true).c_str());
  }

  return s;
}

// TODO need further modification
// still a toy version
size_t ZoneFile::GetUniqueId(char* id, size_t max_size) {
  /* Based on the posix fs implementation */
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

//  struct stat buf;
//  int fd = 0; 
//
//  std::vector<ZonedBlockDevice*> zbds = hbd_->zbds();
//  fd = zbds[0]->GetReadFD();
//  
//  int result = fstat(fd, &buf);
//  if (result == -1) {
//    return 0;
//  }

  char* rid = id;
  const uint64_t magic_number = 0x40506070;
  rid = EncodeVarint64(rid, magic_number); 
  rid = EncodeVarint64(rid, fh_.file_type);
  rid = EncodeVarint64(rid, number_);
  assert(rid >= id);
  return static_cast<size_t>(rid - id);

  return 0;
}


size_t ZonedRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return zone_file_->GetUniqueId(id, max_size);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
