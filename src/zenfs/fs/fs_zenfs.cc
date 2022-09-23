// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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

Status Superblock::DecodeFrom(Slice* input) {
  if (input->size() != ENCODED_SIZE) {
    return Status::Corruption("ZenFS Superblock",
                              "Error: Superblock size missmatch");
  }

  GetFixed32(input, &magic_);
  memcpy(&uuid_, input->data(), sizeof(uuid_));
  input->remove_prefix(sizeof(uuid_));
  GetFixed32(input, &sequence_);
  GetFixed32(input, &version_);
  GetFixed32(input, &flags_);
  GetFixed32(input, &nr_zbds_);

  for (uint32_t i = 0; i < nr_zbds_; i++) {
    uint32_t tmp;
    GetFixed32(input, &tmp);
    block_sizes_.push_back(tmp);
    GetFixed32(input, &tmp);
    zone_sizes_in_blk_.push_back(tmp);
    GetFixed32(input, &tmp);
    nrs_zones_.push_back(tmp);
  }

  GetFixed32(input, &finish_treshold_);
  memcpy(&aux_fs_path_, input->data(), sizeof(aux_fs_path_));
  input->remove_prefix(sizeof(aux_fs_path_));
  memcpy(&reserved_, input->data(), sizeof(input->size()));
  input->remove_prefix(sizeof(input->size()));
  assert(input->size() == 0);

  if (magic_ != MAGIC)
    return Status::Corruption("ZenFS Superblock", "Error: Magic missmatch");
  if (version_ != CURRENT_VERSION)
    return Status::Corruption("ZenFS Superblock", "Error: Version missmatch");

  return Status::OK();
}

void Superblock::EncodeTo(std::string* output) {
  sequence_++; /* Ensure that this superblock representation is unique */
  output->clear();
  PutFixed32(output, magic_);
  output->append(uuid_, sizeof(uuid_));
  PutFixed32(output, sequence_);
  PutFixed32(output, version_);
  PutFixed32(output, flags_);
  PutFixed32(output, nr_zbds_);

  assert(nr_zbds_ > 0);

  for (uint32_t i = 0; i < nr_zbds_; i++) {
    PutFixed32(output, block_sizes_[i]);
    PutFixed32(output, zone_sizes_in_blk_[i]);
    PutFixed32(output, nrs_zones_[i]);
  }
  PutFixed32(output, finish_treshold_);
  output->append(aux_fs_path_, sizeof(aux_fs_path_));
  output->append(reserved_, ENCODED_SIZE - output->length());
  assert(output->length() == ENCODED_SIZE);
}

Status Superblock::CompatibleWith(HybridBlockDevice* hbd) {
  if (nr_zbds_ != hbd->GetNrZonedBlockDevices()) {
    return Status::Corruption("ZenFS Superblock",
        "Error: Number of device missmatch");
  }

  std::vector<uint32_t> block_sizes = hbd->GetBlockSizes();
  std::vector<uint32_t> zone_sizes_in_blk = hbd->GetZoneSizesInBlk();
  std::vector<uint32_t> nrs_zones = hbd->GetNrsZones();

  for (uint32_t i = 0; i < nr_zbds_; i++) {
    if (block_sizes_[i] != block_sizes[i])
      return Status::Corruption("ZenFS Superblock",
                                "Error: block size missmatch");
    if (zone_sizes_in_blk_[i] != zone_sizes_in_blk[i])
      return Status::Corruption("ZenFS Superblock",
                                "Error: zone size missmatch");
    if (nrs_zones_[i] != nrs_zones[i]) { // TODO modify to "larger than"
      fprintf(stderr, "%d vs %d\n", (int)nrs_zones_[i], (int)nrs_zones[i]);
      return Status::Corruption("ZenFS Superblock",
                                "Error: nr of zones missmatch");
    }
  }

  return Status::OK();  // Not implemented
}

IOStatus ZenMetaLog::AddRecord(const Slice& slice) {
  uint32_t record_sz = slice.size();
  const char* data = slice.data();
  size_t phys_sz;
  uint32_t crc = 0;
  char* buffer;
  int ret;
  IOStatus s;

  phys_sz = record_sz + zMetaHeaderSize;

  if (phys_sz % bs_) phys_sz += bs_ - phys_sz % bs_;

  assert(data != nullptr);
  assert((phys_sz % bs_) == 0);

  ret = posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), phys_sz);
  if (ret) return IOStatus::IOError("Failed to allocate memory");

  memset(buffer, 0, phys_sz);

  crc = crc32c::Extend(crc, (const char*)&record_sz, sizeof(uint32_t));
  crc = crc32c::Extend(crc, data, record_sz);
  crc = crc32c::Mask(crc);

  EncodeFixed32(buffer, crc);
  EncodeFixed32(buffer + sizeof(uint32_t), record_sz);
  memcpy(buffer + sizeof(uint32_t) * 2, data, record_sz);

  s = zone_->Append(buffer, phys_sz);
  if (!s.ok()) {
    fprintf(stderr, "ZenMetaLog AddRecord failed: %s\n", 
	s.ToString().c_str());
  }

  free(buffer);
  return s;
}

// Read data from zone to slice
IOStatus ZenMetaLog::Read(Slice* slice) {
//  int f = zbd_->GetReadFD();
  const char* data = slice->data();
  size_t read = 0;
  size_t to_read = slice->size();
  int ret;

  if (read_pos_ >= zone_->wp()) {
    // EOF
    slice->clear();
    return IOStatus::OK();
  }

  if ((read_pos_ + to_read) > (zone_->start() + zone_->max_capacity())) {
    return IOStatus::IOError("Read across zone");
  }

  while (read < to_read) {
    ret = zone_->PositionedRead((char*)data + read, read_pos_, to_read - read);
//    ret = pread(f, (void*)(data + read), to_read - read, read_pos_);

    if (ret <= 0) {
      fprintf(stderr, "pread error on offset 0x%lx error %d %s (ret %d)\n", 
          read_pos_, errno, strerror(errno), (int)ret);
      exit(1);
    }

    if (ret > 0 && ret != (int)to_read) {
      fprintf(stderr, "pread error on offset 0x%lx error %d %s (n %d ret %d)\n", 
          read_pos_, errno, strerror(errno), (int)to_read, (int)ret);
    }

    if (ret == -1 && errno == EINTR) continue;
    if (ret < 0) return IOStatus::IOError("Read failed");

    read += ret;
    read_pos_ += ret;
  }

  return IOStatus::OK();
}

IOStatus ZenMetaLog::ReadRecord(Slice* record, std::string* scratch) {
  Slice header;
  uint32_t record_sz = 0;
  uint32_t record_crc = 0;
  uint32_t actual_crc;
  IOStatus s;

  scratch->clear();
  record->clear();

  scratch->append(zMetaHeaderSize, 0);
  header = Slice(scratch->c_str(), zMetaHeaderSize);

  s = Read(&header);
  if (!s.ok()) return s;

  // EOF?
  if (header.size() == 0) {
    record->clear();
    return IOStatus::OK();
  }

  GetFixed32(&header, &record_crc);
  GetFixed32(&header, &record_sz);

  scratch->clear();
  scratch->append(record_sz, 0);

  *record = Slice(scratch->c_str(), record_sz);
  s = Read(record);
  if (!s.ok()) return s;

  actual_crc = crc32c::Value((const char*)&record_sz, sizeof(uint32_t));
  actual_crc = crc32c::Extend(actual_crc, record->data(), record->size());

  if (actual_crc != crc32c::Unmask(record_crc)) {
    return IOStatus::IOError("Not a valid record");
  }

  /* Next record starts on a block boundary */
  if (read_pos_ % bs_) read_pos_ += bs_ - (read_pos_ % bs_);

  return IOStatus::OK();
}

ZenFS::ZenFS(HybridBlockDevice* hbd, std::shared_ptr<FileSystem> aux_fs,
             std::shared_ptr<Logger> logger)
    : FileSystemWrapper(aux_fs), hbd_(hbd), logger_(logger) {

  Info(logger_, "ZenFS initializing");
  Info(logger_, "ZenFS parameters: block device: %s, aux filesystem: %s",
       hbd->fused_filename().c_str(), target()->Name());

  Info(logger_, "ZenFS initializing");

  zops_ = hbd->zenfs_options();
  hbd->SetZenFS(this);

  next_file_id_ = 1;
  metadata_writer_.zenFS = this;
  cache_ = hbd->cache();

  daemon_stop_ = false;
  level_m_ = zops_.level_m;
}

ZenFS::~ZenFS() {
  Status s;
  Info(logger_, "ZenFS shutting down");

  daemon_stop_ = true;

  hbd_->LogZoneUsage();
  LogFiles();

  meta_log_.reset(nullptr);
  ClearFiles();

  hbd_->stop_ = true;

  if (daemon_) {
    daemon_->join();
  }
  if (daemon_cache_) {
    daemon_cache_->join();
  }

  delete hbd_;
}

void ZenFS::LogFiles() { 
  std::map<std::string, ZoneFile*>::iterator it;
  uint64_t total_size = 0;

  std::map<uint64_t, uint64_t> ssd_level_2_nr; 
  std::map<uint64_t, uint64_t> hdd_level_2_nr; 

  for (it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    std::vector<ZoneExtent*> extents = zFile->GetExtents();

    Info(logger_, "    %-20s sz %lu lh %d lvl %d tp %d id %ld num %ld %s", 
//        it->first.c_str(),
        zFile->sfilename().c_str(),
        zFile->file_size(), zFile->GetWriteLifeTimeHint(), 
        zFile->level(), zFile->file_type(),
        zFile->id(), zFile->number(), 
        num_2_files_.count(zFile->number()) ? "in-sst" : "");

    // Calculate the summary
    for (unsigned int i = 0; i < extents.size(); i++) {
      ZoneExtent* extent = extents[i];
      Zone* z = extent->zone();
      Info(logger_, "          Extent %u fstart 0x%lx zone %lu len %u\n", i,
           extent->fstart(),
           (uint64_t)(z->fstart() / z->max_capacity()), 
           // TODO use physical
           extent->length());

      if (i == 0 && (zFile->is_sst() || zFile->is_blob())) {
        if (!z->rotational()) {
          ssd_level_2_nr[zFile->level()]++; 
        } else {
          hdd_level_2_nr[zFile->level()]++;
        }
      }

      total_size += extent->length();
    }
  }

  Info(logger_, "SSD files:\n");
  for (auto& it0 : ssd_level_2_nr) {
    Info(logger_, "L%ld %ld\n", it0.first, it0.second);
  }
  Info(logger_, "HDD files:\n");
  for (auto& it0 : hdd_level_2_nr) {
    Info(logger_, "L%ld %ld\n", it0.first, it0.second);
  }

  Info(logger_, "Sum of all files: %lu MB of data \n",
       total_size / (1024 * 1024));
}

void ZenFS::ClearFiles() {
  std::map<std::string, ZoneFile*>::iterator it;
  files_mtx_.lock();
  for (it = files_.begin(); it != files_.end(); it++) delete it->second;
  files_.clear();
  num_2_files_.clear();
  files_mtx_.unlock();
}

/* Assumes that files_mutex_ is held */
IOStatus ZenFS::WriteSnapshotLocked(ZenMetaLog* meta_log) {
  IOStatus s;
  std::string snapshot;

  EncodeSnapshotTo(&snapshot);
  s = meta_log->AddRecord(snapshot);
  if (s.ok()) {
    for (auto it = files_.begin(); it != files_.end(); it++) {
      ZoneFile* zone_file = it->second;
      zone_file->MetadataSynced();
    }
  }
  return s;
}

IOStatus ZenFS::WriteEndRecord(ZenMetaLog* meta_log) {
  std::string endRecord;

  PutFixed32(&endRecord, kEndRecord);
  return meta_log->AddRecord(endRecord);
}

/* Assumes the files_mtx_ is held */
IOStatus ZenFS::RollMetaZoneLocked() {
  ZenMetaLog* new_meta_log;
  Zone *new_meta_zone, *old_meta_zone;
  IOStatus s;

  new_meta_zone = hbd_->AllocateMetaZone();
  if (!new_meta_zone) {
    assert(false);  // TMP
    Error(logger_, "Out of metadata zones, we should go to read only now.");
    return IOStatus::NoSpace("Out of metadata zones");
  }

  Info(logger_, "Rolling to metazone %d\n", (int)new_meta_zone->zone_nr());
  new_meta_log = new ZenMetaLog(new_meta_zone->zbd(), new_meta_zone);

  old_meta_zone = meta_log_->GetZone();
  old_meta_zone->open_for_write_ = false;

  /* Write an end record and finish the meta data zone if there is space left */
  if (old_meta_zone->capacity()) WriteEndRecord(meta_log_.get());
  if (old_meta_zone->capacity()) old_meta_zone->Finish();

  meta_log_.reset(new_meta_log);

  std::string super_string;
  superblock_->EncodeTo(&super_string);

  s = meta_log_->AddRecord(super_string);
  if (!s.ok()) {
    Error(logger_,
          "Could not write super block when rolling to a new meta zone");
    return IOStatus::IOError("Failed writing a new superblock");
  }

  s = WriteSnapshotLocked(meta_log_.get());

  /* We've rolled successfully, we can reset the old zone now */
  if (s.ok()) old_meta_zone->Reset();

  return s;
}

IOStatus ZenFS::PersistSnapshot(ZenMetaLog* meta_writer) {
  IOStatus s;

  files_mtx_.lock();
  metadata_sync_mtx_.lock();

  s = WriteSnapshotLocked(meta_writer);
  if (s == IOStatus::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZoneLocked();
  }

  if (!s.ok()) {
    Error(logger_,
          "Failed persisting a snapshot, we should go to read only now!");
  }

  metadata_sync_mtx_.unlock();
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::PersistRecord(std::string record) {
  IOStatus s;

  metadata_sync_mtx_.lock();
  s = meta_log_->AddRecord(record);
  if (s == IOStatus::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZoneLocked();
    /* After a successfull roll, a complete snapshot has been persisted
     * - no need to write the record update */
  }
  metadata_sync_mtx_.unlock();

  return s;
}

IOStatus ZenFS::SyncFileMetadata(ZoneFile* zone_file) {
  std::string fileRecord;
  std::string output;

  IOStatus s;

  files_mtx_.lock();

  PutFixed32(&output, kFileUpdate);
  zone_file->EncodeUpdateTo(&fileRecord);
  PutLengthPrefixedSlice(&output, Slice(fileRecord));

  s = PersistRecord(output);
  if (s.ok()) zone_file->MetadataSynced();

  files_mtx_.unlock();

  return s;
}

// Assert files_mtx_ is held.
IOStatus ZenFS::SyncMigrationMetadata(ZoneFile* zone_file) {
  std::string fileRecord;
  std::string output;

  IOStatus s;

  PutFixed32(&output, kFileMigration);
  zone_file->EncodeSnapshotTo(&fileRecord);
  PutLengthPrefixedSlice(&output, Slice(fileRecord));

  s = PersistRecord(output);
  if (s.ok()) zone_file->MetadataSynced();
  
  return s;
}

ZoneFile* ZenFS::GetFile(std::string fname) {
  ZoneFile* zone_file = nullptr;
  files_mtx_.lock();
  if (files_.find(fname) != files_.end()) {
    zone_file = files_[fname];
  }
  files_mtx_.unlock();
  return zone_file;
}

IOStatus ZenFS::DeleteFile(std::string fname) {

  ZoneFile* zone_file = nullptr;
  IOStatus s;

  zone_file = GetFile(fname);
  files_mtx_.lock();

  if (zone_file != nullptr) {
    Info(logger_, "DEBUG: ZFS::DeleteFile() fname = %s,"
        " level(%d)\n", fname.c_str(), zone_file->level());

    std::string record;

    zone_file = files_[fname];
    files_.erase(fname);

    if (zone_file->is_sst()) {
      num_2_files_.erase(zone_file->number());
    }

    EncodeFileDeletionTo(zone_file, &record);
    s = PersistRecord(record);
    if (!s.ok()) {
      /* Failed to persist the delete, return to a consistent state */
      files_.insert(std::make_pair(fname.c_str(), zone_file));
      if (zone_file->is_sst()) {
        num_2_files_.insert(std::make_pair(zone_file->number(), zone_file));
      }
    } else {
      delete (zone_file);
    }
  } else {
    Info(logger_, "DEBUG: ZFS::DeleteFile() no such file %s\n", fname.c_str());
  }

  files_mtx_.unlock();
  return s;
}

// Debug
uint64_t ZenFS::GetSSTFileTotalSize(std::string fname, uint64_t& num, double& avg_size, 
    int level, uint64_t& level_num) {
  num = 0;
  level_num = 0;
  uint64_t file_size = 0;
  for (auto& it : files_) {
    if (it.first.find("sst") != std::string::npos && it.first != fname) {
      num++;
      file_size += it.second->file_size();

      if (it.second->level() == level) {
        level_num++;
      }
    }
  }
  avg_size = (num == 0) ? 0 : (double)file_size / num;
  return file_size;
}

uint64_t ZenFS::GetBlobFileTotalSize(std::string fname, uint64_t& num, double& avg_size) {
  num = 0;
  uint64_t file_size = 0;
  for (auto& it : files_) {
    if (it.first.find("blob") != std::string::npos && it.first != fname) {
      num++;
      file_size += it.second->file_size();
    }
  }
  avg_size = (num == 0) ? 0 : (double)file_size / num;
  return file_size;
}

IOStatus ZenFS::NewSequentialFile(const std::string& fname,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSSequentialFile>* result,
                                  IODebugContext* dbg) {
  ZoneFile* zone_file = GetFile(fname);

  Debug(logger_, "New sequential file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_reads);

  if (zone_file == nullptr) {
    return target()->NewSequentialFile(ToAuxPath(fname), file_opts, result,
                                       dbg);
  }

  result->reset(new ZonedSequentialFile(zone_file, file_opts));
  return IOStatus::OK();
}

IOStatus ZenFS::NewRandomAccessFile(const std::string& fname,
                                    const FileOptions& file_opts,
                                    std::unique_ptr<FSRandomAccessFile>* result,
                                    IODebugContext* dbg) {
  ZoneFile* zone_file = GetFile(fname);

  Debug(logger_, "New random access file: %s id %ld num %ld direct: %d\n", 
      fname.c_str(), zone_file->id(), zone_file->number(), 
      file_opts.use_direct_reads);

  if (zone_file == nullptr) {
    return target()->NewRandomAccessFile(ToAuxPath(fname), file_opts, result,
                                         dbg);
  }

  result->reset(new ZonedRandomAccessFile(files_[fname], file_opts));
  return IOStatus::OK();
}

// Create file
IOStatus ZenFS::NewWritableFile(const std::string& fname,
                                const FileOptions& file_opts,
                                std::unique_ptr<FSWritableFile>* result,
                                IODebugContext* /*dbg*/) {
  ZoneFile* zone_file;
  IOStatus s;

  int nfi = next_file_id_;
  Debug(logger_, "New writable file %s id %d direct %d level %d\n", 
      fname.c_str(), nfi, file_opts.use_direct_writes, (int)file_opts.level);

  if (GetFile(fname) != nullptr) {
    s = DeleteFile(fname);
    if (!s.ok()) return s;
  }

  zone_file = new ZoneFile(hbd_, fname, file_opts, next_file_id_++, logger_);
  zone_file->zops_ = zops_;
  zone_file->SetZenFS(this);

  /* Persist the creation of the file */
  s = SyncFileMetadata(zone_file);
  if(!s.ok()) {
    delete zone_file;
    return s;
  }

  // Insert to file list
  files_mtx_.lock();
  files_.insert(std::make_pair(fname.c_str(), zone_file));
  if (zone_file->is_sst()) {
    num_2_files_.insert(std::make_pair(zone_file->number(), zone_file));
  }
  files_mtx_.unlock();

  result->reset(new ZonedWritableFile(hbd_, 
        file_opts, zone_file, &metadata_writer_));

  return s;
}

// TODO fix this function
IOStatus ZenFS::ReuseWritableFile(const std::string& fname,
                                  const std::string& old_fname,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSWritableFile>* result,
                                  IODebugContext* dbg) {
  Debug(logger_, "Reuse writable file: %s old name: %s\n", fname.c_str(),
        old_fname.c_str());

  if (GetFile(fname) == nullptr)
    return IOStatus::NotFound("Old file does not exist");

  return NewWritableFile(fname, file_opts, result, dbg);
}

IOStatus ZenFS::FileExists(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  Debug(logger_, "FileExists: %s \n", fname.c_str());

  if (GetFile(fname) == nullptr) {
    return target()->FileExists(ToAuxPath(fname), options, dbg);
  } else {
    return IOStatus::OK();
  }
}

IOStatus ZenFS::ReopenWritableFile(const std::string& fname,
                                   const FileOptions& options,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) {
  Debug(logger_, "Reopen writable file: %s \n", fname.c_str());

  if (GetFile(fname) != nullptr)
    return NewWritableFile(fname, options, result, dbg);

  return target()->NewWritableFile(fname, options, result, dbg);
}

IOStatus ZenFS::GetChildren(const std::string& dir, const IOOptions& options,
                            std::vector<std::string>* result,
                            IODebugContext* dbg) {
  std::map<std::string, ZoneFile*>::iterator it;
  std::vector<std::string> auxfiles;
  IOStatus s;

  Debug(logger_, "GetChildren: %s \n", dir.c_str());

  target()->GetChildren(ToAuxPath(dir), options, &auxfiles, dbg);
  for (const auto& f : auxfiles) {
    if (f != "." && f != "..") result->push_back(f);
  }

  files_mtx_.lock();
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string fname = it->first;
    if (fname.rfind(dir, 0) == 0) {
      if (dir.back() == '/') {
        fname.erase(0, dir.length());
      } else {
        fname.erase(0, dir.length() + 1);
      }
      // Don't report grandchildren
      if (fname.find("/") == std::string::npos) {
        result->push_back(fname);
      }
    }
  }
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::DeleteFile(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  IOStatus s;
  bool deleted_before = false;

  files_mtx_.lock();
  if (deleted_files_.find(fname) != deleted_files_.end()) {
    deleted_files_.erase(fname);
    deleted_before = true;
  }
  files_mtx_.unlock();

  if (deleted_before) {
    Debug(logger_, "Delete file: %s (deleted before)\n", fname.c_str());
    fprintf(stderr, "Delete file: %s (deleted before)\n", fname.c_str());
    return s;
  }
  
  ZoneFile *zone_file = GetFile(fname);

  Debug(logger_, "Delete file: %s \n", fname.c_str());
  fprintf(stderr, "%.2lf Delete file: %s \n", 
      hbd_->elapsed_time_double(), fname.c_str());

  if (zone_file == nullptr) {
    return target()->DeleteFile(ToAuxPath(fname), options, dbg);
  }

  if (zone_file->IsOpenForWR()) {
    s = IOStatus::Busy("Cannot delete, file open for writing: ", fname.c_str());
  } else {
    s = DeleteFile(fname);
  }

  return s;
}

IOStatus ZenFS::GetFileSize(const std::string& f, const IOOptions& options,
                            uint64_t* size, IODebugContext* dbg) {
  ZoneFile* zone_file;
  IOStatus s;

  Debug(logger_, "GetFileSize: %s \n", f.c_str());

  files_mtx_.lock();
  if (files_.find(f) != files_.end()) {
    zone_file = files_[f];
    *size = zone_file->file_size();
  } else {
    s = target()->GetFileSize(ToAuxPath(f), options, size, dbg);
  }
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::RenameFile(const std::string& f, const std::string& t,
                           const IOOptions& options, IODebugContext* dbg) {
  ZoneFile* zone_file;
  IOStatus s;

  Debug(logger_, "Rename file: %s to %s\n", f.c_str(), t.c_str());

  zone_file = GetFile(f);
  if (zone_file != nullptr) {
    s = DeleteFile(t);
    if (s.ok()) {
      files_mtx_.lock();
      files_.erase(f);
      zone_file->Rename(t);
      files_.insert(std::make_pair(t, zone_file));
      files_mtx_.unlock();

      s = SyncFileMetadata(zone_file);
      if (!s.ok()) {
        /* Failed to persist the rename, roll back */
        Debug(logger_, "Failed to persist the rename, roll back %s and %s\n", 
            f.c_str(), t.c_str());
        files_mtx_.lock();
        files_.erase(t);
        zone_file->Rename(f);
        files_.insert(std::make_pair(f, zone_file));
        files_mtx_.unlock();
      }
    }
  } else {
    s = target()->RenameFile(ToAuxPath(f), ToAuxPath(t), options, dbg);
  }

  return s;
}

void ZenFS::EncodeSnapshotTo(std::string* output) {
  std::map<std::string, ZoneFile*>::iterator it;
  std::string files_string;
  PutFixed32(output, kCompleteFilesSnapshot);
  Info(logger_, "EncodeSnapshotTo\n");

  for (it = files_.begin(); it != files_.end(); it++) {
    std::string file_string;
    ZoneFile* zFile = it->second;

    Info(logger_, "EncodeSnapshotTo file %s\n", zFile->filename().c_str());
    zFile->EncodeSnapshotTo(&file_string);
    PutLengthPrefixedSlice(&files_string, Slice(file_string));
  }
  PutLengthPrefixedSlice(output, Slice(files_string));
}

Status ZenFS::DecodeFileUpdateFrom(Slice* slice) {
  FileOptions file_opts; // TODO set the options
  // file_opts.media = StorageMedia::SSD; 


  ZoneFile* update = new ZoneFile(hbd_, "not_set", file_opts, 0, logger_);
  update->zops_ = zops_;
  update->SetZenFS(this);
  
  uint64_t id;
  Status s;

  s = update->DecodeFrom(slice);
  if (!s.ok()) return s;

  id = update->id();
  if (id >= next_file_id_) next_file_id_ = id + 1;

  /* Check if this is an update to an existing file */
  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    if (id == zFile->id()) {
      std::string oldName = zFile->filename();

      s = zFile->MergeUpdate(update);
      delete update;

      if (!s.ok()) return s;

      if (zFile->filename() != oldName) {
        files_.erase(oldName);
        files_.insert(std::make_pair(zFile->filename(), zFile));
      }

      return Status::OK();
    }
  }

  /* The update is a new file */
  assert(GetFile(update->filename()) == nullptr);
  files_.insert(std::make_pair(update->filename(), update));
  if (update->is_sst()) {
    num_2_files_.insert(std::make_pair(update->number(), update));
  }

  return Status::OK();
}

Status ZenFS::DecodeFileMigrationFrom(Slice* slice) {
  FileOptions file_opts; // TODO set the options
  fprintf(stderr, "DecodeFileMigrationFrom ");
  ZoneFile* update = new ZoneFile(hbd_, "not_set", file_opts, 0, logger_);
  update->zops_ = zops_;
  update->SetZenFS(this);

  uint64_t id, number;
  Status s;

  s = update->DecodeFrom(slice);
  if (!s.ok()) return s;
  if (!update->is_sst()) {
    fprintf(stderr, "Warn file migration: not an SST file %s\n", 
        update->filename().c_str());
  }

  id = update->id();
  number = update->number();

  if (id >= next_file_id_) next_file_id_ = id + 1;

  /* Check if this is an update to an existing file */
  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    if (id == zFile->id()) {
      std::string oldName = zFile->filename();
      fprintf(stderr, "name %s\n", oldName.c_str());

      /* replace the old file directly */
      files_.erase(oldName);
      files_.insert(std::make_pair(update->filename(), update));
      num_2_files_.erase(number);
      num_2_files_.insert(std::make_pair(number, update));
      delete zFile;

      return Status::OK();
    }
  }

  /* The update is a new file */
  assert(GetFile(update->filename()) == nullptr);
  files_.insert(std::make_pair(update->filename(), update));
  num_2_files_.insert(std::make_pair(update->number(), update));

  return Status::OK();
}

Status ZenFS::DecodeFileSetLevelFrom(Slice* slice) {
  uint64_t number;
  int level;

  if (!GetFixed64(slice, &number))
    return Status::Corruption("Zone file set level: number missing");

  if (!GetFixed32(slice, (uint32_t*)&level))
    return Status::Corruption("Zone file set level: level missing");

  ZoneFile* zFile = nullptr;

  Info(logger_, "DecodeFileSetLevelFrom change file %ld to lvl %d\n", 
      number, level);

  if (num_2_files_.count(number)) {
    zFile = num_2_files_[number];
    if (!zFile->is_sst()) { 
      return Status::Corruption("Zone file set level: Not an sst file");
    } else {
      zFile->UpdateLevel(level);
    }

    Info(logger_, "DecodeFileSetLevelFrom file %s number %ld\n", 
        zFile->sfilename().c_str(), zFile->number());
  }

  return Status::OK();
}


Status ZenFS::DecodeSnapshotFrom(Slice* input) {
  Slice slice;
  FileOptions file_opts;
  Info(logger_, "DecodeSnapshotFrom\n");

  assert(files_.size() == 0);

  while (GetLengthPrefixedSlice(input, &slice)) {
    ZoneFile* zone_file = new ZoneFile(hbd_, "not_set", file_opts, 0, logger_);
    Status s = zone_file->DecodeFrom(&slice);
    zone_file->zops_ = zops_;
    zone_file->SetZenFS(this);

    if (!s.ok()) return s;

    Info(logger_, "DecodeSnapshotFrom file %s\n", zone_file->sfilename().c_str());

    files_.insert(std::make_pair(zone_file->filename(), zone_file));
    if (zone_file->is_sst()) {
      num_2_files_.insert(std::make_pair(zone_file->number(), zone_file));
    }

    if (zone_file->id() >= next_file_id_)
      next_file_id_ = zone_file->id() + 1;
  }

  return Status::OK();
}

void ZenFS::EncodeFileDeletionTo(ZoneFile* zone_file, std::string* output) {
  std::string file_string;

  PutFixed64(&file_string, zone_file->id());
  PutLengthPrefixedSlice(&file_string, Slice(zone_file->filename()));

  PutFixed32(output, kFileDeletion);
  PutLengthPrefixedSlice(output, Slice(file_string));
}

Status ZenFS::DecodeFileDeletionFrom(Slice* input) {
  uint64_t id;
  std::string fileName;
  Slice slice;


  if (!GetFixed64(input, &id))
    return Status::Corruption("Zone file deletion: file id missing");

  if (!GetLengthPrefixedSlice(input, &slice))
    return Status::Corruption("Zone file deletion: file name missing");

  fileName = slice.ToString();
  if (files_.find(fileName) == files_.end())
    return Status::Corruption("Zone file deletion: no such file");

  ZoneFile* zone_file = files_[fileName];
  if (zone_file->id() != id)
    return Status::Corruption("Zone file deletion: file ID missmatch");

  Info(logger_, "DecodeFileDeletionFrom file %s\n", fileName.c_str());

  files_.erase(fileName);
  if (zone_file->is_sst()) {
    num_2_files_.erase(zone_file->number());
  }

  delete zone_file;

  return Status::OK();
}

Status ZenFS::RecoverFrom(ZenMetaLog* log) {
  bool at_least_one_snapshot = false;
  std::string scratch;
  uint32_t tag = 0;
  Slice record;
  Slice data;
  Status s;
  bool done = false;

  while (!done) {
    IOStatus rs = log->ReadRecord(&record, &scratch);
    if (!rs.ok()) {
      Error(logger_, "Read recovery record failed with error: %s",
            rs.ToString().c_str());
      return Status::Corruption("ZenFS", "Metadata corruption");
    }

    if (!GetFixed32(&record, &tag)) break;

    if (tag == kEndRecord) break;

    if (!GetLengthPrefixedSlice(&record, &data)) {
      Error(logger_, "No recovery record data, tag = %d, record length = %d, "
          "record = %s\n", 
          (int)tag, (int)record.size(), record.ToString(true).c_str());
      return Status::Corruption("ZenFS", "No recovery record data");
    }

    switch (tag) {
      case kCompleteFilesSnapshot:
        ClearFiles();
        s = DecodeSnapshotFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode complete snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        at_least_one_snapshot = true;
        break;

      case kFileUpdate:
        s = DecodeFileUpdateFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      case kFileDeletion:
        s = DecodeFileDeletionFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file deletion: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      case kFileMigration:
        s = DecodeFileMigrationFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file migration: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      case kFileSetLevel:
        s = DecodeFileSetLevelFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file set level: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      default:
        Warn(logger_, "Unexpected metadata record tag: %u", tag);
        return Status::Corruption("ZenFS", "Unexpected tag");
    }
  }

  if (at_least_one_snapshot)
    return Status::OK();
  else
    return Status::NotFound("ZenFS", "No snapshot found");
}

#define ZENV_URI_PATTERN "zenfs://"

/* Mount the filesystem by recovering form the latest valid metadata zone */
Status ZenFS::Mount(bool readonly) {
  std::vector<Zone*> metazones = hbd_->GetMetaZones();
  std::vector<std::unique_ptr<Superblock>> valid_superblocks;
  std::vector<std::unique_ptr<ZenMetaLog>> valid_logs;
  std::vector<Zone*> valid_zones;
  std::vector<std::pair<uint32_t, uint32_t>> seq_map;

  Status s;

  /* We need a minimum of two non-offline meta data zones */
  if (metazones.size() < 2) {
    Error(logger_,
          "Need at least two non-offline meta zones to open for write");
    return Status::NotSupported();
  }

  /* Find all valid superblocks */
  for (const auto& z : metazones) {
    Debug(logger_, "ZenFS::Mount() metazone at %s\n", 
        z->zbd()->filename().c_str());
    fprintf(stderr, "ZenFS::Mount() metazone at %s\n", 
        z->zbd()->filename().c_str());

    std::unique_ptr<ZenMetaLog> log;
    std::string scratch;
    Slice super_record;

    log.reset(new ZenMetaLog(z->zbd(), z));

    if (!log->ReadRecord(&super_record, &scratch).ok()) continue;

    if (super_record.size() == 0) continue;

    std::unique_ptr<Superblock> super_block;

    super_block.reset(new Superblock());
    s = super_block->DecodeFrom(&super_record);
    if (s.ok()) s = super_block->CompatibleWith(hbd_); // Warning: Not implemented
    if (!s.ok()) return s;

    Info(logger_, "Found OK superblock in zone %lu seq: %u\n", z->zone_nr(),
         super_block->GetSeq());
    fprintf(stderr, "Found OK superblock in zone %lu seq: %u\n", z->zone_nr(),
         super_block->GetSeq());

    seq_map.push_back(std::make_pair(super_block->GetSeq(), seq_map.size()));
    valid_superblocks.push_back(std::move(super_block));
    valid_logs.push_back(std::move(log));
    valid_zones.push_back(z);
  }

  if (!seq_map.size()) return Status::NotFound("No valid superblock found");
  fprintf(stderr, "Find valid super block\n");

  /* Sort superblocks by descending sequence number */
  std::sort(seq_map.begin(), seq_map.end(),
            std::greater<std::pair<uint32_t, uint32_t>>());

  bool recovery_ok = false;
  unsigned int r = 0;

  /* Recover from the zone with the highest superblock sequence number.
     If that fails go to the previous as we might have crashed when rolling
     metadata zone.
  */
  for (const auto& sm : seq_map) {
    uint32_t i = sm.second;
    std::string scratch;
    std::unique_ptr<ZenMetaLog> log = std::move(valid_logs[i]);

    s = RecoverFrom(log.get());
    if (!s.ok()) {
      if (s.IsNotFound()) {
        Warn(logger_,
             "Did not find a valid snapshot, trying next meta zone. Error: %s",
             s.ToString().c_str());
        continue;
      }

      Error(logger_, "Metadata corruption. Error: %s", s.ToString().c_str());
      return s;
    }

    r = i;
    recovery_ok = true;
    meta_log_ = std::move(log);
    break;
  }

  if (!recovery_ok) {
    return Status::IOError("Failed to mount filesystem");
  }

  Info(logger_, "Recovered from zone: %d", (int)valid_zones[r]->zone_nr());
  fprintf(stderr, "Recovered from zone: %d", (int)valid_zones[r]->zone_nr());
  superblock_ = std::move(valid_superblocks[r]);

  hbd_->SetFinishTreshold(superblock_->GetFinishTreshold());

  IOOptions foo;
  IODebugContext bar;
  s = target()->CreateDirIfMissing(superblock_->GetAuxFsPath(), foo, &bar);
  if (!s.ok()) {
    Error(logger_, "Failed to create aux filesystem directory.");
    return s;
  }

  /* Free up old metadata zones, to get ready to roll */
  for (const auto& sm : seq_map) {
    uint32_t i = sm.second;
    /* Don't reset the current metadata zone */
    if (i != r) {
      /* Metadata zones are not marked as having valid data, so they can be
       * reset */
      valid_logs[i].reset();
    }
  }

  if (readonly) {
    Info(logger_, "Mounting READ ONLY");
  } else {
    files_mtx_.lock();
    s = RollMetaZoneLocked();
    if (!s.ok()) {
      files_mtx_.unlock();
      Error(logger_, "Failed to roll metadata zone.");
      return s;
    }
    files_mtx_.unlock();
  }

  Info(logger_, "Superblock sequence %d", (int)superblock_->GetSeq());
  Info(logger_, "Finish threshold %u", superblock_->GetFinishTreshold());
  Info(logger_, "Filesystem mount OK");

  if (!readonly) {
    Info(logger_, "Resetting unused IO Zones..");
//    std::vector<ZonedBlockDevice*> zbds = hbd_->zbds();
//    for (auto it : zbds) {
//      it->ResetUnusedIOZones();
//    }
    Info(logger_, "  Done");
  }

  LogFiles();

  fprintf(stderr, "Token bucket, daemons start\n");
  tb_ = new TokenBucket(16, 1);
  daemon_ = new std::thread([=]{MigrationDaemonRun();});
  daemon_cache_ = new std::thread([=]{CacheDaemonRun();});

  return Status::OK();
}

Status ZenFS::MkFS(std::string aux_fs_path, uint32_t finish_threshold) {
  std::vector<Zone*> metazones = hbd_->GetMetaZones();
  std::unique_ptr<ZenMetaLog> log;
  Zone* meta_zone = nullptr;
  IOStatus s;

  if (aux_fs_path.length() > 255) {
    return Status::InvalidArgument(
        "Aux filesystem path must be less than 256 bytes\n");
  }

  ClearFiles();

//  std::vector<ZonedBlockDevice*> zbds = hbd_->zbds();
//  for (auto zbd : zbds) {
//    zbd->ResetUnusedIOZones();
//  }

  for (const auto mz : metazones) {
    if (mz->Reset().ok()) {
      if (!meta_zone) meta_zone = mz;
    } else {
      Warn(logger_, "Failed to reset meta zone\n");
    }
  }

  if (!meta_zone) {
    return Status::IOError("Not available meta zones\n");
  }

  log.reset(new ZenMetaLog(meta_zone->zbd(), meta_zone));

  Superblock* super = new Superblock(hbd_, aux_fs_path, finish_threshold);
  std::string super_string;
  super->EncodeTo(&super_string);
  delete super;

  s = log->AddRecord(super_string);
  if (!s.ok()) return s;

  /* Write an empty snapshot to make the metadata zone valid */
  s = PersistSnapshot(log.get());
  if (!s.ok()) {
    Error(logger_, "Failed to persist snapshot: %s", s.ToString().c_str());
    return Status::IOError("Failed persist snapshot");
  }

  Info(logger_, "Empty filesystem created");
  return Status::OK();
}

IOStatus ZenFS::AreFilesSame(const std::string& first, const std::string& second,
    const IOOptions& /*options*/, bool* res,
    IODebugContext* /*dbg*/) {
  *res = (first == second);
  return IOStatus::OK();
}

IOStatus ZenFS::SetCfOptions(const MutableCFOptions& /*cf_options*/) {
  return IOStatus::OK();
}

// No exception handler
IOStatus ZenFS::CheckOffsetRotational(uint64_t number, uint64_t offset, bool* result) {
  if (!num_2_files_.count(number)) {
    return IOStatus::NotFound("File not found");
  }

  ZoneFile* zone_file = num_2_files_[number];
  uint64_t dev_offset;
  ZoneExtent* zone_extent = zone_file->GetExtent(offset, &dev_offset);
  if (!zone_extent) {
    return IOStatus::NotFound("Extent not found");
  }
  *result = zone_extent->zone()->rotational();
  return IOStatus::OK(); 
}

std::map<std::string, Env::WriteLifeTimeHint> ZenFS::GetWriteLifeTimeHints() {
  std::map<std::string, Env::WriteLifeTimeHint> hint_map;

  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zone_file = it->second;
    std::string filename = it->first;
    hint_map.insert(std::make_pair(filename, zone_file->GetWriteLifeTimeHint()));
  }

  return hint_map;
}

void ZenFS::CacheDaemonRun() {
  std::set<std::string> deleted;
  bool flip = false;

  fprintf(stderr, "CacheDaemonRun started\n");

  while (daemon_stop_ == false) {
    usleep(500000);
    if (cache_) {
      cache_->Sync();
    }

    if (zops_.enable_aggressive_deletion) {
      files_mtx_.lock();
      for (auto& it : pending_delete_) {
        deleted_files_.insert(it);
        deleted.insert(it);
      }
      pending_delete_.clear();
      files_mtx_.unlock();

      for (auto& it : deleted) {
        DeleteFile(it);
      }
      deleted.clear();
    }

    if (zops_.enable_spandb_data_placement && flip) {
      hbd_->CheckSpanDBMaxLevel();
    }
    flip = !flip;
  }
}

IOStatus ZenFS::SyncSetLevelMetadata(uint64_t number, int level) {
  std::string fileRecord;
  std::string output;

  IOStatus s;

  PutFixed32(&output, kFileSetLevel);
  PutFixed64(&fileRecord, number);
  PutFixed32(&fileRecord, level);
  PutLengthPrefixedSlice(&output, Slice(fileRecord));

  fprintf(stderr, "SyncSetLevelMetadata %ld lvl %d\n", number, level);

  s = PersistRecord(output);
  if (!s.ok()) {
    fprintf(stderr, "Error in SyncSetLevelMetadata: %s\n", s.ToString().c_str());
  }
  
  return s;
}

void ZenFS::UpdateLevel(uint64_t number, int old_l, int new_l) {
  auto it = num_2_files_.find(number);
  ZoneFile* f = nullptr;

  if (it != num_2_files_.end()) {
    f = it->second;
    if (f->level() != old_l) {
      fprintf(stderr, "Warning: File %d %s is not level %d\n", 
          (int)f->number(), f->sfilename().c_str(), old_l);
      return;
    }

    f->UpdateLevel(new_l);
    SyncSetLevelMetadata(number, new_l);
  } else 
      fprintf(stderr, "no such file with id %d\n", (int)number);
}

//#ifndef NDEBUG
static std::string GetLogFilename(std::vector<std::string>& bdevs) {
  std::ostringstream ss;
  time_t t = time(0);
  struct tm* log_start = std::localtime(&t);
  char buf[40];

  std::strftime(buf, sizeof(buf), "%Y-%m-%d_%H:%M:%S.log", log_start);
  ss << DEFAULT_ZENV_LOG_PATH << std::string("zenfs_");
  for (auto & bdev : bdevs) {
    ss << bdev << "_";
  }
  ss << buf;

  return ss.str();
}
//#endif

// bdevnames can be multiple devices, separated by ":"
Status NewZenFS(FileSystem** fs, const std::string& bdevnames) {
  setbuf(stderr, NULL);
  
  std::shared_ptr<Logger> logger;
  std::vector<std::string> bdevnames_vector;
  Status s;

  std::string delimiter = "_", token, str = bdevnames;
  size_t pos = 0;
  while ((pos = str.find(delimiter)) != std::string::npos) {
      token = str.substr(0, pos);
      bdevnames_vector.push_back(token);
      str.erase(0, pos + delimiter.length());
  }
  bdevnames_vector.push_back(str);

//#ifndef NDEBUG
  s = Env::Default()->NewLogger(GetLogFilename(bdevnames_vector), &logger);
  if (!s.ok()) {
    fprintf(stderr, "ZenFS: Could not create logger for %s", 
        GetLogFilename(bdevnames_vector).c_str());
  } else {
    fprintf(stderr, "ZenFS: Created logger for %s, DEBUG_LEVEL = %d\n", 
        GetLogFilename(bdevnames_vector).c_str(), (int)DEBUG_LEVEL);
    logger->SetInfoLogLevel(DEBUG_LEVEL);
  }
//#endif

  HybridBlockDevice* hbd = new HybridBlockDevice(bdevnames_vector, logger);
  IOStatus hbd_status = hbd->Open();
  if (!hbd_status.ok()) {
    Error(logger, "Failed to open zoned block device: %s",
          hbd_status.ToString().c_str());
    return Status::IOError(hbd_status.ToString());
  }

  ZenFS* zenFS = new ZenFS(hbd, FileSystem::Default(), logger);
  s = zenFS->Mount(false);
  if (!s.ok()) {
    fprintf(stderr, "Mount error: %s\n", s.ToString().c_str());
    delete zenFS;
    return s;
  }

  *fs = zenFS;
  return Status::OK();
}

// TODO: Modify this function to get the file systems correctly
std::map<std::string, std::string> ListZenFileSystems() {
  std::map<std::string, std::string> zenFileSystems;
  DIR* dir = opendir("/sys/class/block");
  struct dirent* entry;

  while (NULL != (entry = readdir(dir))) {
    if (entry->d_type == DT_LNK) {
      std::string zbdName = std::string(entry->d_name);
      ZonedBlockDevice* zbd = new ZonedBlockDevice(zbdName, nullptr);
      IOStatus zbd_status = zbd->Open(true);

      if (zbd_status.ok()) {
        std::vector<Zone*> metazones = zbd->GetMetaZones();
//	fprintf(stderr, "DEBUG: ListZenFileSystems() metazones.size() = %ld\n", metazones.size());
        std::string scratch;
        Slice super_record;
        Status s;

        for (const auto z : metazones) {
          Superblock super_block;
          std::unique_ptr<ZenMetaLog> log;
          log.reset(new ZenMetaLog(z->zbd(), z));

          if (!log->ReadRecord(&super_record, &scratch).ok()) continue;
          s = super_block.DecodeFrom(&super_record);
          if (s.ok()) {
            /* Map the uuid to the device-mapped (i.g dm-linear) block device to
               avoid trying to mount the whole block device in case of a split
               device */
            if (zenFileSystems.find(super_block.GetUUID()) !=
                    zenFileSystems.end() &&
                zenFileSystems[super_block.GetUUID()].rfind("dm-", 0) == 0) {
              break;
            }
            zenFileSystems[super_block.GetUUID()] = zbdName;
            break;
          }
        }

        delete zbd;
        continue;
      }
    }
  }

  return zenFileSystems;
}

extern "C" FactoryFunc<FileSystem> zenfs_filesystem_reg;

FactoryFunc<FileSystem> zenfs_filesystem_reg =
    ObjectLibrary::Default()->Register<FileSystem>(
        "zenfs://.*", [](const std::string& uri, std::unique_ptr<FileSystem>* f,
                         std::string* errmsg) {
          std::string devID = uri;
          FileSystem* fs = nullptr;
          Status s;

          devID.replace(0, strlen("zenfs://"), "");
          if (devID.rfind("dev:") == 0) {
            devID.replace(0, strlen("dev:"), "");
            s = NewZenFS(&fs, devID);
            if (!s.ok()) {
              *errmsg = s.ToString();
            }
          } else if (devID.rfind("uuid:") == 0) {
            std::map<std::string, std::string> zenFileSystems =
                ListZenFileSystems();
            devID.replace(0, strlen("uuid:"), "");

            if (zenFileSystems.find(devID) == zenFileSystems.end()) {
              *errmsg = "UUID not found";
            } else {
              s = NewZenFS(&fs, zenFileSystems[devID]);
              if (!s.ok()) {
                *errmsg = s.ToString();
              }
            }
          } else {
            *errmsg = "Malformed URI";
          }
          f->reset(fs);
          return f->get();
        });
};  // namespace ROCKSDB_NAMESPACE

#else

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
Status NewZenFS(FileSystem** /*fs*/, const std::string& /*bdevname*/) {
  return Status::NotSupported("Not built with ZenFS support\n");
}
std::map<std::string, std::string> ListZenFileSystems() {
  std::map<std::string, std::string> zenFileSystems;
  return zenFileSystems;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
