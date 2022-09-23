// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>
#include <sstream>
#include <fstream>
#include <utility>
#include <vector>

#include <linux/fs.h>

#include "util.h"
#include "io_zenfs.h"
#include "rocksdb/env.h"

#define BLKDISCARD _IO(0x12,119)

namespace ROCKSDB_NAMESPACE {

Zone::Zone(ZonedBlockDevice *zbd, struct zbd_zone *z)
    : zbd_(zbd),
      start_(zbd_zone_start(z)),
      fstart_(zbd_zone_start(z)),
      max_capacity_(zbd_zone_capacity(z)),
      wp_(zbd_zone_wp(z)),
      fwp_(zbd_zone_wp(z)),
      open_for_write_(false) {
  ts_ = gettime();
  wrfd_ = -1;

  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  rand_read_times_ = 0;

  capacity_ = 0;
  if (!(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z))) {
    if (max_capacity_ >= (zbd_zone_wp(z) - zbd_zone_start(z))) {
      capacity_ = max_capacity_ - (zbd_zone_wp(z) - zbd_zone_start(z));
    }
  }

  if (zbd_zone_cond(z) == 14) {
    capacity_ = 0;
  }

//    capacity_ = zbd_zone_capacity(z) - (zbd_zone_wp(z) - zbd_zone_start(z));
}

Zone::Zone(ZonedBlockDevice* zbd, uint64_t start, 
    uint64_t capacity, uint64_t wp) : zbd_(zbd) {
  start_ = fstart_ = start;
  max_capacity_ = capacity;
  wrfd_ = -1;
  wp_ = fwp_ = 
    (wp < start || wp >= start + capacity ? start : wp);
  capacity_ = max_capacity_ - (wp_ - start_); 

  open_for_write_ = false;

  ts_ = gettime();

  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  rand_read_times_ = 0;
}

Zone::Zone(ZonedBlockDevice* zbd, std::shared_ptr<FileSystem> fs, 
    std::string& posix_path, uint64_t start, uint32_t capacity) : 
    zbd_(zbd), fs_(fs), posix_path_(posix_path) {

  filename_ = posix_path + "_" + std::to_string(start);

  start_ = fstart_ = start;
  max_capacity_ = capacity;

  {
    struct stat st;
    int ret = stat(filename_.c_str(), &st);
    if (ret == 0) {
      fprintf(stderr, "z fn %s size %ld\n", filename_.c_str(), st.st_size); 
      wp_ = fwp_ = start + st.st_size; 
    } else { 
      wp_ = fwp_ = start; 
    }
  }
  capacity_ = max_capacity_ - (wp_ - start_); 

  wrfd_ = -1;

  open_for_write_ = false;

  ts_ = gettime();

  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  rand_read_times_ = 0;
}

void Zone::PrintCond() {
  struct zbd_zone z2;
  int fd = zbd_->GetWriteFD();
  unsigned int report;
  zbd_report_zones(fd, start_, max_capacity_, ZBD_RO_ALL, &z2, &report);
  fprintf(stderr, "cond: %d (rep %d) start 0x%lx (0x%lx) wp 0x%lx (0x%lx) max 0x%lx\n", 
      zbd_zone_cond(&z2), report, (uint64_t)zbd_zone_start(&z2), start_,
      (uint64_t)zbd_zone_wp(&z2), wp_, max_capacity_);
}

IOStatus Zone::PhysicalReset() {
  size_t zone_sz = zbd_->zone_sz();
  int fd = zbd_->GetWriteFD();
  int ret;

  // Zoned device
  if (zbd_->zoned_) {
    fprintf(stderr, "Zoned reset nr %ld\n", zone_nr());

    ret = zbd_reset_zones(fd, start_, zone_sz);

    if (ret) {
      fprintf(stderr, "Zone reset failed, error %d, %s, num %ld\n", 
          errno, strerror(errno), zone_nr());
      return IOStatus::IOError("Zone reset failed\n");
    }

//    if (zbd_zone_offline(&z))
//      capacity_ = 0;
//    else
      capacity_ = max_capacity_;
    //max_capacity_ = capacity_ = zbd_zone_capacity(&z);

    return IOStatus::OK();
  }

  // Conventional device

  if (fs_ == nullptr) {
    fprintf(stderr, "Non-Zoned reset %ld\n", zone_nr());

    uint64_t range[2];
    range[0] = start_;
    range[1] = max_capacity_;
    
    ret = ::ioctl(fd, BLKDISCARD, &range);

    if (ret < 0) {
      fprintf(stderr, "Trim failed, error %d, %s, num %ld\n", 
          errno, strerror(errno), zone_nr());
      return IOStatus::IOError("Zone reset failed\n");
    }

    capacity_ = max_capacity_;
    return IOStatus::OK();
  }

  // File system
  {
    fprintf(stderr, "File system reset nr %ld fn %s cap %ld\n", 
        zone_nr(), filename_.c_str(), capacity_);
    IOOptions opts;
    IODebugContext* dbg = nullptr;

    if (wrfd_ > 0) {
      close(wrfd_);
      wrfd_ = 0;
    }

    if (fs_->FileExists(filename_, opts, dbg).ok()) {
      IOStatus s = fs_->DeleteFile(filename_, opts, dbg);
      if (!s.ok()) {
        fprintf(stderr, "DeleteFile failed filename %s\n", filename_.c_str());
      } 
    } else {
      fprintf(stderr, "reset empty %lu (nr %d)\n", start_, (int)zone_nr());
    }
    capacity_ = max_capacity_;
  }

  return IOStatus::OK();
}

IOStatus Zone::Reset() {
  double reset_lifetime;

  assert(!IsUsed());

  IOStatus ios = PhysicalReset();
  if (!ios.ok()) {
    return ios;
  }

  // Set lifetime
  reset_lifetime = lifespan();
  ts_ = reset_lifetime + ts_; 

  // Set write pointers
  wp_ = start_;
  fwp_ = fstart_;

  // Set hints
  lifetime_ = Env::WLTH_NOT_SET;
  file_type_ = kTempFile;
  level_ = 999;

  rand_read_times_ = 0;
  size_compacted_ = 0;

  // A bug fix
  open_for_write_ = false;
  used_capacity_ = 0;

  used_mtx_.lock();
  levels_.clear();
  used_sizes_.clear();
  used_mtx_.unlock();

  return IOStatus::OK();
}

Zone::~Zone() {
//  IOOptions opts;
//  IODebugContext* dbg = nullptr;
//
//  if (wr_file != nullptr) {
//    fprintf(stderr, "Close zone file: %s\n", filename_.c_str());
//    wr_file->Close(opts, dbg);
//  }

  if (wrfd_ > 0) {
    close(wrfd_);
    wrfd_ = 0;
  }
}

IOStatus Zone::Finish() {
  size_t zone_sz = zbd_->zone_sz();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(!open_for_write_);

  // Zoned device
  if (zbd_->zoned_) {
    fprintf(stderr, "Finish zoned nr %ld\n", zone_nr());
    if (!rotational()) {
      ret = zbd_finish_zones(fd, start_, zone_sz);
      if (ret) {
        fprintf(stderr, "%s finish failed\n", zbd_->filename().c_str());
        return IOStatus::IOError("Zone finish failed\n");
      }
    }
    wp_ = start_ + zone_sz;
    fwp_ = fstart_ + zone_sz;
  } else if (fs_ == nullptr) {
    // Conventional device. No need to finish
    fprintf(stderr, "Finish conventional nr %ld\n", zone_nr());
    wp_ = start_ + max_capacity_;
    fwp_ = fstart_ + max_capacity_;
  } else {
    fprintf(stderr, "Finish file system nr %ld\n", zone_nr());
    // posix file system 
//    if (wr_file) {
//      IOOptions opts;
//      IODebugContext* dbg = nullptr;
//
//      IOStatus s = wr_file->Close(opts, dbg);
//      if (!s.ok()) {
//        fprintf(stderr, "Close file failed: %s %s\n", 
//            filename_.c_str(), s.ToString().c_str());
//      } 
//      wr_file = nullptr;
//    } else {
//      fprintf(stderr, "No wr_file ptr: zone %d\n", (int)zone_nr());
//    }

//    if (wrfd_ > 0) {
//      close(wrfd_);
//      fprintf(stderr, "Close file: %s\n", filename_.c_str());
//      wrfd_ = 0;
//    }
    
    wp_ = start_ + max_capacity_;
    fwp_ = fstart_ + max_capacity_;
  }

  fprintf(stderr, "Zone finish num %ld addr 0x%lx\n", 
      zone_nr(), start_);

  capacity_ = 0;

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  size_t zone_sz = zbd_->zone_sz();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(!open_for_write_);

  // Emulated zones
  if (!zbd_->zoned_) return IOStatus::OK();

  if (!(IsEmpty() || capacity_ == 0) && !rotational()) {
    ret = zbd_close_zones(fd, start_, zone_sz);
    fprintf(stderr, "Zone close num %d start 0x%lx cap 0x%lx sz 0x%lx\n", 
        (int)zone_nr(), start_, capacity_, zone_sz);
    if (ret) {
      fprintf(stderr, "Zone close failed, dev %s\n"
          "error %d, %s, num %d, ret %d\n", 
          zbd_->filename().c_str(), 
          errno, strerror(errno), (int)zone_nr(), ret);
      //      return IOStatus::IOError("Zone close failed\n");
    }
  }

  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size, double* latency) {
  char *ptr = data;
  int ret;
  double t1, t2;

  if (size == 0) {
    fprintf(stderr, "Warning: appending size zero zone_nr %ld\n", zone_nr()); 
  }

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  if (IsEmpty()) {
    ts_ = gettime(); 
  }

  t1 = gettime();

  if (fs_ == nullptr) {
    // zoned (real or emulated)
    int fd = zbd_->GetWriteFD();
    uint64_t left = size;

    assert((size % zbd_->GetBlockSize()) == 0);
    while (left) {
      ret = pwrite(fd, ptr, left, wp_);

      if (ret <= 0) {
        fprintf(stderr, "Zone pwrite failed, error %d, %s, "
            "num %d (0x%lx + %ld), ret %d\n", 
            errno, strerror(errno), (int)zone_nr(), wp_, left, ret);
        return IOStatus::IOError("Write failed");
      }

      ptr += ret;
      wp_ += ret;
      fwp_ += ret;
      capacity_ -= ret;
      left -= ret;
    }
  } else {
//    IODebugContext* dbg = nullptr;
//    IOStatus s;
//    if (wr_file == nullptr) {
//      FileOptions opts;
//      fprintf(stderr, "file system open file %s\n", filename_.c_str());
//      
//      s = fs_->NewWritableFile(filename_, opts, &wr_file, dbg); 
//      if (!s.ok()) {
//        fprintf(stderr, "NewWritableFile failed %s\n", filename_.c_str());
//        return IOStatus::Corruption("Open file failed\n");
//      }
//    }
//
//    IOOptions opts;
//    s = wr_file->Append(Slice(data, size), opts, dbg);
//    if (!s.ok()) {
//      fprintf(stderr, "File append failed %s, [%ld --- %ld + %ld]\n", 
//          filename_.c_str(), start_, wp_, (uint64_t)size);
//    } else {
//      fprintf(stderr, "File append success %s, [%ld --- %ld + %ld]\n", 
//          filename_.c_str(), start_, wp_, (uint64_t)size);
//    }

    if (wrfd_ <= 0) {
      wrfd_ = open(filename_.c_str(), O_RDWR | O_DIRECT | O_CREAT, 0666);
      if (wrfd_ <= 0) {
        fprintf(stderr, "[ERROR] Zone::Append: open file %s failed\n", 
            filename_.c_str()); 
        exit(1);
      }
    }

    ret = pwrite(wrfd_.load(), data, size, wp_ - start_);

    if (ret <= 0) {
      fprintf(stderr, "[ERROR] Zone::Append pread %s error %d %s\n", 
          filename_.c_str(), errno, strerror(errno));
      exit(1);
    }

//    fprintf(stderr, "File append ret %d %s, [%ld --- %ld + %ld] [%s]\n", 
//        ret, filename_.c_str(), start_, wp_, (uint64_t)size,
//        Slice(data, 20).ToString(true).c_str());

    wp_ += size;
    fwp_ += size;
    capacity_ -= size;
  }

  t2 = gettime();
  if (latency) {
    *latency = t2 - t1;
  }

  return IOStatus::OK();
}

ssize_t Zone::PositionedRead(char* scratch, uint64_t offset, size_t n) {
  int ret;
  if (!fs_) {
    int f = zbd_->GetReadFD();
    ret = pread(f, (void*)scratch, n, offset);
    return ret;
  }

//  IOStatus s;
//  IODebugContext* dbg = nullptr;
//
//  if (rd_file == nullptr) {
//    FileOptions opts;
//
//    s = fs_->NewRandomAccessFile(filename_, opts, &rd_file, dbg); 
//    if (!s.ok()) {
//      fprintf(stderr, "zone positioned read: %s open failed\n", 
//          filename_.c_str());
//      return 0;
//    }
//  }
//
//  IOOptions opts;
//  Slice result;
//  s = rd_file->Read(offset - start_, n, opts, &result, scratch, dbg); 
//  if (!s.ok()) {
//    fprintf(stderr, "zone positioned read: %s read failed, offset %ld, "
//        "n %ld %s\n", 
//        filename_.c_str(), offset, n, s.ToString().c_str());
//    return 0;
//  }

//  fprintf(stderr, "file system %s offset %ld n %ld\n", filename_.c_str(), 
//      offset, n);

  if (wrfd_ <= 0) {
    wrfd_ = open(filename_.c_str(), O_RDWR | O_DIRECT | O_CREAT, 0666);
    if (wrfd_ <= 0) {
      fprintf(stderr, "zone positioned read: %s open RDONLY failed\n", 
          filename_.c_str());
      return 0;
    }
  }

  void* data = nullptr;  //  -- aligned_offset
  uint32_t blksz = zbd_->GetBlockSize();

  uint64_t aligned_offset = offset / blksz * blksz; 
  uint64_t aligned_length = 
    ((offset + n - 1) / blksz + 1) * blksz - aligned_offset;

  ret = posix_memalign(&data, sysconf(_SC_PAGESIZE), aligned_length);
  if (ret) {
    fprintf(stderr, "in read: posix_memalign failed ret %d %s\n",
        ret, strerror(errno));
    return 0;
  }

  ret = pread(wrfd_, data, aligned_length, aligned_offset - start_); 
  if (ret <= 0) {
    int t = wrfd_;
    fprintf(stderr, "pread failed %s fd %d ao %lx al %ld n %ld "
        "offset %lx errno %d %s\n", 
        filename_.c_str(), t, aligned_offset, aligned_length, n,
        offset, errno, strerror(errno));
  } else {
    memcpy(scratch, (char*)data + offset - aligned_offset, n);
    ret = n;
  }

  free(data);

//  fprintf(stderr, "pread ret %d %s fd %d [%s]\n", 
//      ret, filename_.c_str(), wrfd_, 
//      n < 20 ? Slice(scratch, n).ToString(true).c_str() : "" );

  return ret;
}

uint64_t Zone::zone_nr() { 
  if (fs_) return start_ / max_capacity_;
  if (zbd_->zoned_) return start_ / zbd_->zone_sz(); 
  return start_ / zbd_->zone_cap_;
}

bool Zone::zoned() {
  return zbd_->zoned_;
}

void Zone::SetFstart(uint64_t fstart) { 
  fstart_ = fstart; 
  fwp_ = fstart_ - start_ + wp_;
}

void Zone::CloseWR() {
  assert(open_for_write_);
  open_for_write_ = false;

  if (!(IsEmpty() || IsFull())) {
    Status s = Close();
    if (!s.ok()) {
      fprintf(stderr, "Problem in close: %s\n", s.ToString().c_str());
      exit(1);
    }
  } else if (IsEmpty()) {
    fprintf(stderr, "Warning: Closing an empty zone nr %d, cap %ld\n", 
        (int)zone_nr(), capacity());
  } 

  // Full is also OK; don't need to do physical close
  zbd_->PutOpenIOZoneToken();
  hbd_->PutOpenIOZoneToken();

  if (IsFull()) {
    zbd_->PutActiveIOZoneToken();
    hbd_->PutActiveIOZoneToken();
  }
}

double Zone::lifespan() {
  return gettime() - ts_;
}

void Zone::SetLifespan(double lf) {
  ts_ = gettime() - lf;
}

double Zone::rand_read_freq() {
  int rt = rand_read_times_;
  return (double) rt / lifespan();
}

bool Zone::rotational() {
  return zbd_->rotational(); 
}

void Zone::IncFileIdLevel(uint64_t id, int lvl, uint64_t size) {
  used_mtx_.lock();
  if (used_sizes_.find(id) == used_sizes_.end()) {
    used_sizes_[id] = size;
    levels_[lvl]++;
  } else {
    used_sizes_[id] += size;
  }
  if (levels_.size() == 1) {
    SetLevel(lvl);
  } 

  used_mtx_.unlock();
}

void Zone::DecFileIdLevel(uint64_t id, int lvl, uint64_t size) {
  used_mtx_.lock();
  if (used_sizes_.find(id) != used_sizes_.end()) {
    used_sizes_[id] -= size;
    if (used_sizes_[id] == 0) {
      levels_[lvl]--;
      if (levels_[lvl] <= 0) {
        levels_.erase(lvl);
      }

      used_sizes_.erase(id);
    }
  } 
  used_mtx_.unlock();
}

ZoneExtent::ZoneExtent(uint64_t start, uint32_t length, Zone *zone)
    : fstart_(start), length_(length), zone_(zone) {
  ref = 0;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
