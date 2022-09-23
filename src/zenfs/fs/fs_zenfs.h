// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include "file/filename.h"

#include <thread>

#include "io_zenfs.h"
#include "util.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

class Superblock {
  uint32_t magic_ = 0;
  char uuid_[37] = {0};
  uint32_t sequence_ = 0;
  uint32_t version_ = 0;
  uint32_t flags_ = 0;
  uint32_t nr_zbds_ = 0;
  std::vector<uint32_t> block_sizes_;
  std::vector<uint32_t> zone_sizes_in_blk_;
  std::vector<uint32_t> nrs_zones_;
  char aux_fs_path_[256] = {0};
  uint32_t finish_treshold_ = 0;
  char reserved_[187] = {0};

 public:
  const uint32_t MAGIC = 0x5a454e46; /* ZENF */
  const uint32_t ENCODED_SIZE = 512;
  const uint32_t CURRENT_VERSION = 2; /* 1 */
  const uint32_t DEFAULT_FLAGS = 0;

  Superblock() {}

  /* Create a superblock for a filesystem covering the entire zoned block device
   */
  Superblock(HybridBlockDevice* hbd, std::string aux_fs_path = "",
             uint32_t finish_threshold = 0) {
    std::string uuid = Env::Default()->GenerateUniqueId();
    int uuid_len =
        std::min(uuid.length(),
                 sizeof(uuid_) - 1); /* make sure uuid is nullterminated */
    memcpy((void*)uuid_, uuid.c_str(), uuid_len);
    magic_ = MAGIC;
    version_ = CURRENT_VERSION;
    flags_ = DEFAULT_FLAGS;
    finish_treshold_ = finish_threshold;

    nr_zbds_ = hbd->GetNrZonedBlockDevices();
    block_sizes_ = hbd->GetBlockSizes();
    zone_sizes_in_blk_ = hbd->GetZoneSizesInBlk();
    nrs_zones_ = hbd->GetNrsZones();

    strncpy(aux_fs_path_, aux_fs_path.c_str(), sizeof(aux_fs_path_) - 1);
  }

  Status DecodeFrom(Slice* input);
  void EncodeTo(std::string* output);
  Status CompatibleWith(HybridBlockDevice* zbd);

  uint32_t GetSeq() { return sequence_; }
  std::string GetAuxFsPath() { return std::string(aux_fs_path_); }
  uint32_t GetFinishTreshold() { return finish_treshold_; }
  std::string GetUUID() { return std::string(uuid_); }
};

class ZenMetaLog {
  uint64_t read_pos_;
  Zone* zone_;
  ZonedBlockDevice* zbd_;
  size_t bs_;

  /* Every meta log record is prefixed with a CRC(32 bits) and record length (32
   * bits) */
  const size_t zMetaHeaderSize = sizeof(uint32_t) * 2;

 public:
  ZenMetaLog(ZonedBlockDevice* zbd, Zone* zone) {
    zbd_ = zbd;
    zone_ = zone;
    zone_->open_for_write_ = true;
    bs_ = zbd_->GetBlockSize();
    read_pos_ = zone->start();
  }

  virtual ~ZenMetaLog() { zone_->open_for_write_ = false; }

  IOStatus AddRecord(const Slice& slice);
  IOStatus ReadRecord(Slice* record, std::string* scratch);

  Zone* GetZone() { return zone_; };

 private:
  IOStatus Read(Slice* slice);
};

class ZenFS : public FileSystemWrapper {
  HybridBlockDevice* hbd_;

  ZenFSOptions zops_;

  int flag_blob = 0;

  std::map<std::string, ZoneFile*> files_;
  std::map<uint64_t, ZoneFile*> num_2_files_;
  std::mutex files_mtx_;
  std::shared_ptr<Logger> logger_;
  std::atomic<uint64_t> next_file_id_;

  std::unique_ptr<ZenMetaLog> meta_log_;
  std::mutex metadata_sync_mtx_;
  std::unique_ptr<Superblock> superblock_;

  std::shared_ptr<Logger> GetLogger() { return logger_; }

  uint64_t total_read_bytes_ = 0;
  uint64_t total_write_bytes_ = 0;

  struct MetadataWriter : public ZonedWritableFile::MetadataWriter {
    ZenFS* zenFS;
    IOStatus Persist(ZoneFile* zoneFile) {
      Debug(zenFS->GetLogger(), "Syncing metadata for: %s",
            zoneFile->sfilename().c_str());
      return zenFS->SyncFileMetadata(zoneFile);
    }
  };

  MetadataWriter metadata_writer_;

  std::thread *daemon_ = nullptr, *daemon_cache_ = nullptr;
  CacheManager* cache_ = nullptr;

  std::atomic<bool> daemon_stop_;

  // debug
  std::map<int, uint64_t> stat_bytes_;
  std::map<int, int> level2num_;

  // compaction info
  std::map<int, std::map<int, int>> lvl_and_output_lvl_to_zones_;
  std::map<int, int> total_lvl_generated_;
  std::map<int, int> lvl_generated_;
  uint64_t wal_sizes_;

  std::map<int, std::map<int, int>> medium_lvl_allocation_;
  std::map<int, int> current_allocation_;
  std::map<int, int> level_capacities_;
  std::map<int, int> storage_demands_;
  int total_storage_demands_;
  int level_m_;
  int ssts_in_ssd_level_m_;

  // migration info
  enum MigrationStatus : uint8_t {
    kOnGoing = 0,
    kAborted = 1,
    kFinished = 5,
  };
  TokenBucket* tb_;

  // dangerous
  std::set<std::string> deleted_files_;
  std::set<std::string> pending_delete_;

  enum ZenFSTag : uint32_t {
    kCompleteFilesSnapshot = 1,
    kFileUpdate = 2,
    kFileDeletion = 3,
    kEndRecord = 4,
    kFileMigration = 5,
    kFileSetLevel = 6,
  };

  void LogFiles();
  void ClearFiles();
  IOStatus WriteSnapshotLocked(ZenMetaLog* meta_log);
  IOStatus WriteEndRecord(ZenMetaLog* meta_log);
  IOStatus RollMetaZoneLocked();
  IOStatus PersistSnapshot(ZenMetaLog* meta_writer);
  IOStatus PersistRecord(std::string record);
  IOStatus SyncFileMetadata(ZoneFile* zoneFile);

  void EncodeSnapshotTo(std::string* output);
  void EncodeFileDeletionTo(ZoneFile* zoneFile, std::string* output);

  Status DecodeSnapshotFrom(Slice* input);
  Status DecodeFileUpdateFrom(Slice* slice);
  Status DecodeFileDeletionFrom(Slice* slice);
  Status DecodeFileMigrationFrom(Slice* slice);
  Status DecodeFileSetLevelFrom(Slice* slice);

  Status RecoverFrom(ZenMetaLog* log);

  std::string ToAuxPath(std::string path) {
    return superblock_->GetAuxFsPath() + path;
  }

  std::string ToZenFSPath(std::string aux_path) {
    std::string path = aux_path;
    path.erase(0, superblock_->GetAuxFsPath().length());
    return path;
  }

  ZoneFile* GetFile(std::string fname);
  IOStatus DeleteFile(std::string fname);

  // compaction hints, implemented in fs_compaction_hint.cc
  void ComputeCurrentAllocation(bool print);
  void ComputeLevelM(bool print);
  void ComputeStorageDemands(bool print);

  // migration, implemented in fs_mig_zenfs.cc
  void MigrationDaemonRun();
  int ComparePriority(ZoneFile* f1, ZoneFile* f2);
  ZoneFile* TrySelectSsdObject();
  ZoneFile* TrySelectHddObject();
  bool MaybeCapacityCalibration();
  bool MaybePopularityCalibration();
  bool H2ZoneStylePopularityMigration();
  bool TrivialStylePopularityMigration();
  MigrationStatus SelectAndMoveHddToSsd(MigrationStatus ms, 
      int max_level = BLOB_FILE_LEVEL);
  MigrationStatus SelectAndMoveSsdToHdd(MigrationStatus ms);
  Status PerformMigration(ZoneFile* zf, bool to_rotational);


  // Debug
  uint64_t GetBlobFileTotalSize(std::string fname, uint64_t& num, double& avg_size);
  uint64_t GetSSTFileTotalSize(std::string fname, uint64_t& num, double& avg_size,
      int level, uint64_t& level_num);
//  uint64_t AddStatBytes(bool is_read, uint64_t bytes);

 public:
  explicit ZenFS(HybridBlockDevice* zbd, std::shared_ptr<FileSystem> aux_fs,
                 std::shared_ptr<Logger> logger);
  virtual ~ZenFS();

  Status Mount(bool readonly);
  Status MkFS(std::string aux_fs_path, uint32_t finish_threshold);
  std::map<std::string, Env::WriteLifeTimeHint> GetWriteLifeTimeHints();

  // compaction hints, implemented in fs_compaction_hint.cc 
  void ComputeCompactionHintedInfo(bool print);
  int LevelMAvailableZones();
  bool CanPutInLevelM();
  int GetLevelM() { return level_m_; }

  const char* Name() const override {
    return "ZenFS - The Zoned-enabled File System";
  }

  IOStatus InformExtentChange(uint64_t src, uint64_t dest, uint64_t length);

  virtual IOStatus NewSequentialFile(const std::string& fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSSequentialFile>* result,
                                     IODebugContext* dbg) override;
  virtual IOStatus NewRandomAccessFile(
      const std::string& fname, const FileOptions& file_opts,
      std::unique_ptr<FSRandomAccessFile>* result,
      IODebugContext* dbg) override;
  virtual IOStatus NewWritableFile(const std::string& fname,
                                   const FileOptions& file_opts,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) override;
  virtual IOStatus ReuseWritableFile(const std::string& fname,
                                     const std::string& old_fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSWritableFile>* result,
                                     IODebugContext* dbg) override;
  virtual IOStatus ReopenWritableFile(const std::string& fname,
                                      const FileOptions& options,
                                      std::unique_ptr<FSWritableFile>* result,
                                      IODebugContext* dbg) override;
  virtual IOStatus FileExists(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;
  virtual IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                               std::vector<std::string>* result,
                               IODebugContext* dbg) override;
  virtual IOStatus DeleteFile(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;
  IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                       uint64_t* size, IODebugContext* dbg) override;
  IOStatus RenameFile(const std::string& f, const std::string& t,
                      const IOOptions& options, IODebugContext* dbg) override;

  IOStatus GetFreeSpace(const std::string& /*path*/,
                        const IOOptions& /*options*/, uint64_t* diskfree,
                        IODebugContext* /*dbg*/) override {
    *diskfree = hbd_->GetFreeSpace();
    return IOStatus::OK();
  }

  // The directory structure is stored in the aux file system

  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override {
    if (GetFile(path) != nullptr) {
      *is_dir = false;
      return IOStatus::OK();
    }
    return target()->IsDirectory(ToAuxPath(path), options, is_dir, dbg);
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    Debug(logger_, "NewDirectory: %s to aux: %s\n", name.c_str(),
          ToAuxPath(name).c_str());
    return target()->NewDirectory(ToAuxPath(name), io_opts, result, dbg);
  }

  IOStatus CreateDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    return target()->CreateDir(ToAuxPath(d), options, dbg);
  }

  IOStatus CreateDirIfMissing(const std::string& d, const IOOptions& options,
                              IODebugContext* dbg) override {
    IOStatus s = target()->CreateDirIfMissing(ToAuxPath(d), options, dbg);
    return s;
  }

  IOStatus DeleteDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    std::vector<std::string> children;
    IOStatus s;

    Debug(logger_, "DeleteDir: %s aux: %s\n", d.c_str(), ToAuxPath(d).c_str());

    s = GetChildren(d, options, &children, dbg);
    if (children.size() != 0)
      return IOStatus::IOError("Directory has children");

    return target()->DeleteDir(ToAuxPath(d), options, dbg);
  }

  // We might want to override these in the future
  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override {
    return target()->GetAbsolutePath(ToAuxPath(db_path), options, output_path,
                                     dbg);
  }

  IOStatus LockFile(const std::string& f, const IOOptions& options,
                    FileLock** l, IODebugContext* dbg) override {
    return target()->LockFile(ToAuxPath(f), options, l, dbg);
  }

  IOStatus UnlockFile(FileLock* l, const IOOptions& options,
                      IODebugContext* dbg) override {
    return target()->UnlockFile(l, options, dbg);
  }

  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override {
    return target()->NewLogger(ToAuxPath(fname), options, result, dbg);
  }

  // Not supported (at least not yet)
  IOStatus Truncate(const std::string& /*fname*/, size_t /*size*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("Truncate is not implemented in ZenFS");
  }

  virtual IOStatus NewRandomRWFile(const std::string& /*fname*/,
                                   const FileOptions& /*options*/,
                                   std::unique_ptr<FSRandomRWFile>* /*result*/,
                                   IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("RandomRWFile is not implemented in ZenFS");
  }

  virtual IOStatus NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) override {
    return IOStatus::NotSupported(
        "MemoryMappedFileBuffer is not implemented in ZenFS");
  }

  IOStatus GetFileModificationTime(const std::string& /*fname*/,
                                   const IOOptions& /*options*/,
                                   uint64_t* /*file_mtime*/,
                                   IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported(
        "GetFileModificationTime is not implemented in ZenFS");
  }

  virtual IOStatus LinkFile(const std::string& /*src*/,
                            const std::string& /*target*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("LinkFile is not supported in ZenFS");
  }

  virtual IOStatus NumFileLinks(const std::string& /*fname*/,
                                const IOOptions& /*options*/,
                                uint64_t* /*count*/, IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "Getting number of file links is not supported in ZenFS");
  }

  virtual IOStatus AreFilesSame(const std::string& first,
                                const std::string& second,
                                const IOOptions& /*options*/, bool* res,
                                IODebugContext* /*dbg*/); 

  virtual IOStatus SetCfOptions(const MutableCFOptions& cf_options);
  IOStatus CheckOffsetRotational(uint64_t file_id, uint64_t offset, bool* result);

  IOStatus SyncMigrationMetadata(ZoneFile* zoneFile);

  IOStatus SyncSetLevelMetadata(uint64_t file_id, int level);

  void CacheDaemonRun();

  virtual void UpdateLevel(uint64_t file_id, int old_l, int new_l) override;
};
#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)

Status NewZenFS(FileSystem** fs, const std::string& bdevname);
std::map<std::string, std::string> ListZenFileSystems();

}  // namespace ROCKSDB_NAMESPACE
