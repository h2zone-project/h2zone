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

namespace ROCKSDB_NAMESPACE {

Status ZoneFile::BeginMigration(bool is_rotational) {
  if (fh_.is_deleted) {
    return Status::Aborted();
  }

  if (IsOpenForWR()) {
    fprintf(stderr, "Error: You cannot migrate an open file!\n");
    exit(1);
  }

  fh_.is_migrated = true;
  fh_.is_rotational = is_rotational;

  mig_size_left_ = file_size();
  new_extents_.clear();
  migrated_size_ = 0;
  migrated_extent_ = nullptr;

  return Status::OK();
}

bool ZoneFile::MigrationNotFinished() {
  return (mig_size_left_ > 0);
}

void ZoneFile::EndMigration(const char* /*reason*/) {
  if (mig_size_left_ != 0) {
    extents_mtx_.lock();
    ReleaseExtents(false);
    extents_mtx_.unlock();
  } else {
    // TODO deal with the metadata
    extents_mtx_.lock();
    PushExtent();
    ReleaseExtents();

    for (auto e = std::begin(new_extents_); 
        e != std::end(new_extents_); ++e) {
      extents_.push_back(*e);
    }
    new_extents_.clear();
    extents_mtx_.unlock();

    zenfs_->SyncMigrationMetadata(this);
  }

  mig_size_left_ = 0;
  fh_.is_migrated = false;
  if (!fh_.is_deleted) {
    CloseWR();
  }
}

// To avoid directly deleting a file that is being migrated
void ZoneFile::SetDeleted() {
  fh_.is_deleted = true;
}

bool ZoneFile::IsBeingMigrated() {
  return fh_.is_migrated;
}

bool ZoneFile::IsBeingDeleted() {
  return fh_.is_deleted;
}

// When return not "OK", all the following migrations stop.
Status ZoneFile::Move(int data_size) {
  if (!fh_.is_migrated) {
    fprintf(stderr, "Migration stop: This file is not migrating\n");
    return Status::Aborted();
  }

  if (fh_.is_deleted) {
    fprintf(stderr, "Migration stop: the file is being deleted.\n");
    return Status::Aborted();
  }

  uint32_t left = data_size;
  uint64_t pad_sz = 0;

  if (left > mig_size_left_) {
    uint64_t block_sz = hbd_->GetMaximumBlockSize();
    uint64_t align = mig_size_left_ % block_sz;
    if (align) {
      pad_sz = block_sz - align;
    }

    left = mig_size_left_ + pad_sz;
  }

  uint32_t wr_size, offset = 0;
  Status s;

  double latency;

  void* data;
  int ret = posix_memalign((void**)&data, sysconf(_SC_PAGESIZE), data_size);

  if (ret) {
    return Status::Aborted("buffer allocation failed");
  }

  // Read
  {
    Slice result;
    IOOptions opts;
    IOStatus s2 = PositionedRead(migrated_size_, data_size, opts, 
       &result, (char*)data, true); 

    if (!s2.ok()) {
      free(data);
      return Status::Aborted(s2.ToString().c_str());
    }

    if (pad_sz) {
      if (mig_size_left_ + pad_sz > (uint64_t)data_size) {
        fprintf(stderr, "pad too much! %ld + %ld > %d\n", 
            mig_size_left_, pad_sz, data_size);
      }
      memset((char*)data + mig_size_left_, 0x0, pad_sz);
    }
  }

  // Write: Find a zone first
  if (active_zone_ == NULL) {
    active_zone_ = hbd_->AllocateZone(fh_); 

    if (!active_zone_) {
      return Status::NoSpace("Zone allocation failure 1\n");
    }

    if (migrated_extent_) {
      return Status::Aborted("Migrated extent should be null\n");
    }

    extent_fstart_ = active_zone_->fwp();
    extent_filepos_ = migrated_size_;
    migrated_extent_ = new ZoneExtent(extent_fstart_, 0, active_zone_);
  }

  // Write: Start to write 
  while (left > 0) {
    if (active_zone_->capacity() == 0) {
      extents_mtx_.lock();
      PushExtent();
      extents_mtx_.unlock();

      active_zone_->CloseWR();
      active_zone_ = hbd_->AllocateZone(fh_); 

      if (!active_zone_) {
        return Status::NoSpace("Zone allocation failure 2\n");
      }

      extent_fstart_ = active_zone_->fwp();
      extent_filepos_ = migrated_size_;
      migrated_extent_ = new ZoneExtent(extent_fstart_, 0, active_zone_);
    }

    wr_size = left;
    if (wr_size > active_zone_->capacity()) {
      wr_size = active_zone_->capacity();
    }

    // change both wp_ and fwp()
    s = active_zone_->Append((char*)data + offset, wr_size, &latency); 

    if (!s.ok()) {
      Debug(logger_, "ZoneFile Move failed status %s\n",
          s.ToString().c_str());
      free(data);
      return s;
    }

    left -= wr_size;
    offset += wr_size;

    if (mig_size_left_ < wr_size) {
      migrated_size_ += mig_size_left_;
      mig_size_left_ = 0;
    } else {
      migrated_size_ += wr_size;
      mig_size_left_ -= wr_size;
    }
    migrated_extent_->SetLength(migrated_size_ - extent_filepos_);

    hbd_->AddStatBytes(active_zone_->rotational(), 
        false, 
        false, 
        false, fh_.level, wr_size, latency, 
        active_zone_->zone_nr());
  }

  free(data);
  return Status::OK();
}


}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
