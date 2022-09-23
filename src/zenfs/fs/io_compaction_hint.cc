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

void ZoneFile::SanitizeHints() {
  // Calculate file num and file type
  WalFileType wft;

  int pos = static_cast<int>(filename_.find_last_of('/'));
  std::string s = filename_.substr(pos + 1, filename_.length() - pos - 1);
  ParseFileName(s.c_str(), &number_, "abc", &(fh_.file_type), &wft); 

  if (fh_.file_type == kBlobFile) { // Blob file
    fh_.level = BLOB_FILE_LEVEL; 
  } else if (fh_.file_type == kWalFile) {  // WAL file 
    fh_.level = BLOB_FILE_LEVEL + 1; 
  } else if (fh_.file_type != kTableFile) { // Metadata file
    fh_.level = BLOB_FILE_LEVEL + 2;
  }

  if (ssd_size_ < hdd_size_) rotational_ = true;
}

// What if migration and trivial moving happen at the same time?
void ZoneFile::UpdateLevel(int level) {
  for (auto extent : extents_) {
    Zone* zone = extent->zone();
    zone->DecFileIdLevel(id(), this->level(), extent->length()); 
    zone->IncFileIdLevel(id(), level, extent->length());
  }

  fh_.is_trivial_moved = true;
  fh_.level = level;
}


void ZoneFile::Hint(FSAccessPattern pattern) {
  if (pattern != FSAccessPattern::kSequential) {
    compaction_hints_.is_involved = false;
    // May be a compaction output file. Now the compaction ends.
  } else if (pattern_ != FSAccessPattern::kSequential) {
    // Change to compaction input file.
    for (auto it : extents_) {
      it->zone()->AddCompactedBytes(it->length());
    }
  }
  pattern_ = pattern;
}

void ZoneFile::CompactionHint(const CompactionHints& hint) {
  Info(logger_, "Compaction hint: %s %ld num %ld lvl %d start %d -> output %d %s\n", 
      sfilename().c_str(), id(), number(), level(),
      hint.start_level, hint.output_level, 
      (hint.is_input) ? "input" : "output");
  compaction_hints_ = hint;
}


uint64_t ZoneFile::compaction_process() {
  return compaction_process_;
}

double ZoneFile::compaction_process_pct() {
  return (double)compaction_process_ / fileSize;
}

bool ZoneFile::is_compaction_involved() {
  return compaction_hints_.is_involved; 
}

bool ZoneFile::is_compaction_input() {
  return compaction_hints_.is_input;
}

int ZoneFile::compaction_output_level() {
  if (!is_compaction_involved()) {
    fprintf(stderr, "Warning: not selected but requested for output level\n");
    return 999;
  }
  return compaction_hints_.output_level;
}

int ZoneFile::compaction_job() {
  return compaction_hints_.job_id;
}

void ZonedWritableFile::CompactionHint(const CompactionHints& hint) {
  zone_file_->CompactionHint(hint);
}

void ZonedRandomAccessFile::Hint(AccessPattern pattern) {
  zone_file_->Hint(pattern);
}

void ZonedRandomAccessFile::CompactionHint(const CompactionHints& hint) {
  zone_file_->CompactionHint(hint);
}


}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
