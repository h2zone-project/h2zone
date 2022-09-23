// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/file_system.h"
#include "file/filename.h"

#include "util.h"
#include "zbd_zenfs.h"
#include "zenfs_options.h"


namespace ROCKSDB_NAMESPACE {

class CacheManager;
using FSAccessPattern = FSRandomAccessFile::AccessPattern;
class ZenFS;

struct FileHints {
  Env::WriteLifeTimeHint lifetime = Env::WriteLifeTimeHint::WLTH_NOT_SET;
  FileType file_type = kTempFile;
  int level = 0;

  bool is_migrated = false;
  bool is_rotational = false; 
  bool is_deleted = false;
  bool is_trivial_moved = false;

  bool is_blob() { return file_type == kBlobFile; }
  bool is_sst() { return file_type == kTableFile; }
  bool is_wal() { return file_type == kWalFile; }
};

class ZoneExtent {
 private:
  uint64_t fstart_;
  uint32_t length_;
  Zone* zone_;

 public:
  std::atomic<int> ref;
  explicit ZoneExtent(uint64_t start, uint32_t length, Zone* zone);
  Status DecodeFrom(Slice* input);
  void EncodeTo(std::string* output);

  uint64_t fstart() { return fstart_; }
  uint32_t length() { return length_; }
  Zone* zone() { return zone_; }

  void IncRef() { 
    ref++; 
//    fprintf(stderr, "IncRef zone %ld fstart %ld len %d ref %d\n", 
//        (zone_) ? zone_->zone_nr() : -1, 
//        fstart_, length_, ref.load());
  }

  void DecRef() { 
    ref--; 
//    fprintf(stderr, "DecRef zone %ld fstart %ld len %d ref %d\n", 
//        (zone_) ? zone_->zone_nr() : -1, 
//        fstart_, length_, ref.load());
    if (ref.load() < 0) {
      fprintf(stderr, "Error: ref smaller than zero! zone %ld fstart %ld len %d\n", 
        (zone_) ? zone_->zone_nr() : -1, fstart_, length_);
      exit(1);
    }
  }

  void SetFstart(uint64_t fstart) { fstart_ = fstart; }
  void SetLength(uint32_t length) { length_ = length; }
  void SetZone(Zone* z) { zone_ = z; }
};

class ZoneFile {
 protected:
  HybridBlockDevice* hbd_;
  FileOptions file_opts_;
  std::vector<ZoneExtent*> extents_;
  Zone* active_zone_;
  uint64_t extent_fstart_; // mapped start
  uint64_t extent_filepos_;  // file position of the extent

  CacheManager* cache_ = nullptr; 

  FileHints fh_;

  uint64_t fileSize;
  std::string filename_;
  uint64_t file_id_;
  uint64_t number_ = 1ull << 30; // Will be same as the number in RocksDB
  uint32_t nr_synced_extents_;
  std::mutex extents_mtx_;

  bool open_for_wr_ = false;
  bool rotational_ = false;
  FSAccessPattern pattern_ = FSAccessPattern::kNormal;

  uint32_t ssd_size_ = 0;
  uint32_t hdd_size_ = 0;

  double create_time_ = 0;
  double close_time_ = 0;
  double delete_time_ = 0;

  uint64_t tot_read_size_ = 0;

  CompactionHints compaction_hints_;
  bool compaction_finished_;
  uint64_t compaction_process_ = 0;

  ZenFS* zenfs_ = nullptr;
  
  // migration
  std::vector<ZoneExtent*> new_extents_;
  uint64_t mig_size_left_ = 0;
  uint64_t migrated_size_ = 0;
  ZoneExtent* migrated_extent_ = nullptr;

 public:
  std::shared_ptr<Logger> logger_;
  ZenFSOptions zops_;
  std::atomic<uint64_t> rand_read_times_;

  explicit ZoneFile(HybridBlockDevice* hbd, std::string filename,
      const FileOptions& file_opts, uint64_t file_id_, std::shared_ptr<Logger>& logger);

  virtual ~ZoneFile();

  void OpenWR();
  void CloseWR();
  bool IsOpenForWR();

  IOStatus Append(void* data, int data_size, int valid_size, 
                  const IOOptions& options);

  std::string sfilename();
  std::string filename();
  void Rename(std::string name);
  uint64_t file_size();
  void SetFileSize(uint64_t sz);

  uint32_t GetBlockSize() { return hbd_->GetMaximumBlockSize(); }
  std::vector<ZoneExtent*> GetExtents() { return extents_; }

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options, 
                          Slice* result, char* scratch, bool direct);
  ZoneExtent* GetExtent(uint64_t file_offset, uint64_t* dev_offset);
  void PushExtent();
  void ReleaseExtents(bool original = true);

  void EncodeTo(std::string* output, uint32_t extent_start);
  void EncodeUpdateTo(std::string* output) {
    EncodeTo(output, nr_synced_extents_);
  };
  void EncodeSnapshotTo(std::string* output) { EncodeTo(output, 0); };
  void MetadataSynced() { nr_synced_extents_ = extents_.size(); };

  Status DecodeFrom(Slice* input);
  Status MergeUpdate(ZoneFile* update);

  bool is_blob() { return file_type() == kBlobFile; }
  bool is_sst() { return file_type() == kTableFile; }
  bool is_wal() { return file_type() == kWalFile; }
  uint64_t id() { return file_id_; }
  uint64_t number() { return number_; }
  size_t GetUniqueId(char* id, size_t max_size);

  HybridBlockDevice* hbd() { return hbd_; }

  // hints
  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime);
  void SetFileType(FileType file_type);
  void SetLevel(int level);
  bool rotational(bool closed = false);
  bool trivial_moved() { return fh_.is_trivial_moved; }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return fh_.lifetime; }
  double lifespan();
  FileType file_type() { return fh_.file_type; }
  int level() { return fh_.level; }

  void SetZenFS(ZenFS* zenfs); 

  // compaction hints, implemented in io_compaction_hint.cc
  void SanitizeHints();
  void UpdateLevel(int level);
  void Hint(FSAccessPattern pattern); 
  FSAccessPattern pattern() { return pattern_; }
  void CompactionHint(const CompactionHints& hint);
  uint64_t compaction_process();
  double compaction_process_pct();
  bool is_compaction_involved();
  bool is_compaction_input();
  int compaction_output_level();
  int compaction_job();

  // migration, implemented in io_mig_zenfs.cc
  Status BeginMigration(bool is_rotational);
  bool MigrationNotFinished();
  void EndMigration(const char* reason = "");
  void SetDeleted();
  bool IsBeingMigrated();
  bool IsBeingDeleted();
  Status Move(int data_size);
  uint64_t SizeLeftForMigration() { return mig_size_left_; }

  // caching, implemented in io_cache.cc
  IOStatus InformCacheRecords(Cache::KeyRecords& key_records); 
  void SetCacheManager(CacheManager* cache);
  IOStatus ReadCachedDataBlock(const IOOptions& opts, uint64_t offset, 
      uint64_t length, bool direct, char** result); 
};

class ZonedWritableFile : public FSWritableFile {
 public:
  /* Interface for persisting metadata for files */
  class MetadataWriter {
   public:
    virtual ~MetadataWriter();
    virtual IOStatus Persist(ZoneFile* zone_file) = 0;
  };

  explicit ZonedWritableFile(HybridBlockDevice* hbd, 
                             const FileOptions& file_opts,
                             ZoneFile* zone_file,
                             MetadataWriter* metadata_writer = nullptr);
  virtual ~ZonedWritableFile();

  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          IODebugContext* dbg) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& opts,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }
  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& options,
                                    IODebugContext* dbg) override;
  virtual IOStatus PositionedAppend(
      const Slice& data, uint64_t offset, const IOOptions& opts,
      const DataVerificationInfo& /* verification_info */,
      IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }
  virtual IOStatus Truncate(uint64_t size, const IOOptions& options,
                            IODebugContext* dbg) override;
  virtual IOStatus Close(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Flush(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  virtual IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                             const IOOptions& options,
                             IODebugContext* dbg) override;
  virtual IOStatus Fsync(const IOOptions& options,
                         IODebugContext* dbg) override;

  bool use_direct_io() const override { return !buffered; }
  bool IsSyncThreadSafe() const override { return true; };
  size_t GetRequiredBufferAlignment() const override {
    return zone_file_->GetBlockSize();
  }
  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;

  virtual Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return zone_file_->GetWriteLifeTimeHint();
  }

  virtual void CompactionHint(const CompactionHints& hint) override;

 private:
  IOStatus BufferedWrite(const Slice& data, const IOOptions& opts);
  IOStatus FlushBuffer();

  bool buffered;
  char* buffer;
  size_t buffer_sz;
  uint32_t block_sz;
  uint32_t buffer_pos;
  uint64_t wp;
  int write_temp;

  ZoneFile* zone_file_;
  MetadataWriter* metadata_writer_;

  FileOptions file_opts_;
  std::mutex buffer_mtx_;
};

class ZonedSequentialFile : public FSSequentialFile {
 private:
  ZoneFile* zone_file_;
  uint64_t rp;
  bool direct_;

 public:
  explicit ZonedSequentialFile(ZoneFile* zone_file, const FileOptions& file_opts)
      : zone_file_(zone_file), rp(0), direct_(file_opts.use_direct_reads) {}

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
  IOStatus Skip(uint64_t n);

  bool use_direct_io() const override { return direct_; };

  size_t GetRequiredBufferAlignment() const override {
    return zone_file_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return IOStatus::OK();
  }
};

class ZonedRandomAccessFile : public FSRandomAccessFile {
 private:
  ZoneFile* zone_file_;
  bool direct_;

 public:
  explicit ZonedRandomAccessFile(ZoneFile* zone_file,
                                 const FileOptions& file_opts)
      : zone_file_(zone_file), direct_(file_opts.use_direct_reads) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  bool use_direct_io() const override { return direct_; }

  size_t GetRequiredBufferAlignment() const override {
    return zone_file_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return IOStatus::OK();
  }

  size_t GetUniqueId(char* id, size_t max_size) const override;

  IOStatus InformCacheRecords(Cache::KeyRecords& /*key_records*/) override;

  virtual void Hint(AccessPattern pattern) override; 

  virtual void CompactionHint(const CompactionHints& hint) override;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
