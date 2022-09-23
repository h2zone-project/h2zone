#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include <set>

#include "rocksdb/env.h" 
#include "util.h"

namespace ROCKSDB_NAMESPACE {

double gettime() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_sec + (double)tv.tv_usec / 1000000.0; 
}

TokenBucket::TokenBucket(int second, int token_each_time) {
  time1_ = gettime(); 
  time2_ = time1_;
  second_ = second;
  token_each_time_ = token_each_time;
  tokens_ = token_each_time;
}

bool TokenBucket::AcquireToken() {
  time2_ = gettime();

  if (tokens_ > 0) {
    tokens_--;
    return true;
  }

  if (time2_ - time1_ >= (double)second_) {
    time1_ = time2_;
    tokens_ = token_each_time_ - 1;
    return true;
  }

  return false;
}

ZenStat::ZenStat(ZenFSOptions& zops) {
  total_cnt = std::vector<uint64_t>(8, 0);
  total_bytes = std::vector<uint64_t>(8, 0);
  zops_ = zops;

  time1 = gettime(); 
  extract_ts = time1;
}

int ZenStat::index(bool is_rotational, bool is_read, 
    bool is_compaction_read, int level) {
  int sum = 0;
  if (is_rotational) sum += 4;
  if (!is_read) sum += 2;
  if (is_read && is_compaction_read) sum++; 
  if (!is_read && level == BLOB_FILE_LEVEL + 1) sum++; 
  return sum;
}

uint64_t ZenStat::total(std::vector<uint64_t>& v, std::vector<int> indices) {
  uint64_t sum = 0;
  for (auto i : indices) {
    sum += v[i];
  }
  return sum;
}


void ZenStat::AddStatBytes(bool is_rotational, bool is_read, 
    bool is_compaction_read, bool is_index, int level, uint64_t bytes, 
    double latency, int zone_number) {

  stat_mtx_.lock();

  // for spandb
  hdd_rwtotal += (is_rotational) ? bytes : 0;
  ssd_rwtotal += (is_rotational) ? 0 : bytes;

  int ind = index(is_rotational, is_read, is_compaction_read, level);

  if (level < BLOB_FILE_LEVEL && level > max_level) {
    max_level = level;
  }

  if (is_rotational && is_read && !is_compaction_read) {
    hdd_bytes_total += bytes;
  }
  if (is_rotational && is_read) {
    hdd_read_times++;
    hdd_read_times_lvl[level]++;
  }

  if (latency > 0.0) {
    latency *= 1000.0;
    if (is_rotational && is_read && !is_compaction_read) {
      hdd_latency_total += latency;
    }

    double latency_thres = 0.125;

    while (latency > latency_thres) {
      latency_thres *= 1.414214; // 1.2599;
    }

    while ((int)latencies.size() <= ind) {
      latencies.push_back({});
    }

    // Hides: max_level is at least zero.
    while ((int)latencies[ind][latency_thres].size() <= max_level) {
      latencies[ind][latency_thres].push_back(0);
    }

    if (is_rotational) {
      if (!lat2hddzone.count(latency_thres)) {
        lat2hddzone[latency_thres] = {0, 0};
      }
      lat2hddzone[latency_thres].first += zone_number;
      lat2hddzone[latency_thres].second ++;
    }
    
    latencies[ind][latency_thres][(level < BLOB_FILE_LEVEL) ? level : 0]++;

  }

  total_cnt[ind] ++;
  total_bytes[ind] += bytes;
  total_rwc ++;
  total_rwb += bytes;

  // Not simplified yet
  if (is_rotational) {
    if (is_read) {
      hdd_level_2_read_bytes[level] += bytes;
      if (is_compaction_read) {
        hdd_level_2_compaction_read_bytes[level] += bytes;
      } else if (!is_index) {
        hdd_r++;
      }
    } else {
      hdd_level_2_write_bytes[level] += bytes;
      hdd_w += bytes;
    }
  } else {
    if (is_read) {
      ssd_level_2_read_bytes[level] += bytes;
      if (is_compaction_read) {
        ssd_level_2_compaction_read_bytes[level] += bytes;
      }
    } else {
      ssd_level_2_write_bytes[level] += bytes;
      ssd_w += bytes;
      if (level == BLOB_FILE_LEVEL + 1) {
        ssd_wal_w += bytes;
      } 
    }
  }

  if (total_rwc % 50 == 0) {
    CheckWorkload();
  }

  stat_mtx_.unlock();

}

void ZenStat::ExtractThpt(double* ssd_thpt, double* hdd_thpt) {
  double current = gettime();

  if (!ssd_thpt || !hdd_thpt) {
    return;
  }

  stat_mtx_.lock();
  *ssd_thpt = ssd_rwtotal / (current - extract_ts); 
  *hdd_thpt = hdd_rwtotal / (current - extract_ts);
  hdd_rwtotal = 0;
  ssd_rwtotal = 0;
  extract_ts = current;
  stat_mtx_.unlock();
}

void ZenStat::PrintLatencies(std::vector<int> indices) {
  fprintf(stderr, "[%s] NCR CR NWA WA", indices[0] == 0 ? "SSD" : "HDD"); 
  for (int j = 0; j <= max_level; j++) {
    fprintf(stderr, "%5d ", j);
  }
  fprintf(stderr, "\n");

  std::set<double> latencies_set_tmp;

  for (auto i : indices) {
    if (i < (int)latencies.size()) {
      for (auto& it : latencies[i]) {
        latencies_set_tmp.insert(it.first);
      }
    }
  }

  for (auto lat : latencies_set_tmp) {
    fprintf(stderr, "lat. %9.1lf ms  ", lat);
    for (auto i : indices) {
      // No latency records for this operation and the following operations
      if ((int)latencies.size() <= i) {
        continue;
      }

      // No latency records for this operation
      fprintf(stderr, "| ");
      if (!latencies[i].count(lat)) {
        for (int j = 0; j <= max_level; j++) {
          fprintf(stderr, "%5s ", " ");  // spaces 
        }
        continue;
      }

      auto& it = latencies[i][lat];
      for (int j = 0; j <= max_level; j++) {
        if (j < (int)it.size()) {
          fprintf(stderr, "%5d ", it[j]);
        } else {
          fprintf(stderr, "%5s ", " ");
        }
      }
    }

    if (indices[0] > 0 && lat2hddzone.count(lat) && lat2hddzone[lat].second) {
      fprintf(stderr, " (%.3lf) ", 
          (double)lat2hddzone[lat].first / lat2hddzone[lat].second);
    }

    fprintf(stderr, "\n");
  }

  lat2hddzone.clear();
  for (auto i : indices) {
    if (i < (int)latencies.size()) {
      latencies[i].clear();
    }
  }
}

void ZenStat::CheckWorkload() {
  double time0 = gettime();

  if (time0 - time1 >= 30.0) {
    uint64_t interval_w = ssd_w + hdd_w;
    double hdd_ratio = (double)hdd_w / (interval_w + 0.00001);
    double mx_thpt = (hdd_ratio * zops_.hdd_write_bound +
        (1.0 - hdd_ratio) * zops_.ssd_write_bound);
    double ssd_w_thpt = ssd_w / (time0 - time1);
    double hdd_w_thpt = hdd_w / (time0 - time1);
    double wal_thpt = ssd_wal_w / (time0 - time1);
    double hdd_r_thpt = hdd_r / (time0 - time1);

    fprintf(stderr, "mx_thpt %3.3lf MiB/s hdd_ratio %3.3lf "
        "ssd_w_thpt %3.3lf MiB/s hdd_w_thpt %3.3lf MiB/s "
        "wal_thpt %3.3lf MiB/s hdd_r_thpt %3.3lf IO/s\n",
        mx_thpt / 1024 / 1024, hdd_ratio, 
        ssd_w_thpt / 1024 / 1024, hdd_w_thpt / 1024 / 1024, 
        wal_thpt / 1024 / 1024, hdd_r_thpt);
    fprintf(stderr, "hdd_latency_total %.3lf hdd_bytes_total %ld KiB "
        "hdd_read_times %ld\n",
        hdd_latency_total, hdd_bytes_total / 1024, 
        hdd_read_times);

    for (auto& it : hdd_read_times_lvl) {
      fprintf(stderr, "Hdd read lvl %d : %d\n", it.first, it.second);
    }

    PrintLatencies({0, 1, 2, 3});
    PrintLatencies({4, 5, 6, 7});

    ssd_w = hdd_w = hdd_r = 0;

    hdd_latency_total = 0;
    hdd_bytes_total = 0;
    hdd_read_times = 0;
    hdd_read_times_lvl.clear();

    if (hdd_r_thpt >= (double)zops_.hdd_randread_bound) {
      fprintf(stderr, "read-intensive\n");
      ws = WorkloadStatus::kReadIntensive;
    } else if (ssd_w_thpt > zops_.ssd_write_bound / 10 || 
        hdd_w_thpt > zops_.hdd_write_bound / 10) {
      fprintf(stderr, "write-intensive\n");
      ws = WorkloadStatus::kWriteIntensive;
    } else {
      fprintf(stderr, "normal workload\n");
      ws = WorkloadStatus::kNormal;
    }

    time1 = time0;
    printable_ = true;
  }
}

void ZenStat::LogLevelBytes(Logger* logger) {
  for (auto& it : ssd_level_2_read_bytes) {
    Debug(logger, "ssd read bytes level %d 0x%lx %lu B %lu MB\n",
        (int)it.first, it.second, it.second, it.second / 1024 / 1024);
  }
  for (auto& it : ssd_level_2_compaction_read_bytes) {
    Debug(logger, "ssd cread bytes level %d 0x%lx %lu B %lu MB\n",
        (int)it.first, it.second, it.second, it.second / 1024 / 1024);
  }
  for (auto& it : ssd_level_2_write_bytes) {
    Debug(logger, "ssd write bytes level %d 0x%lx %lu B %lu MB\n",
        (int)it.first, it.second, it.second, it.second / 1024 / 1024);
  }
  for (auto& it : hdd_level_2_read_bytes) {
    Debug(logger, "hdd read bytes level %d 0x%lx %lu B %lu MB\n",
        (int)it.first, it.second, it.second, it.second / 1024 / 1024);
  }
  for (auto& it : hdd_level_2_compaction_read_bytes) {
    Debug(logger, "hdd cread bytes level %d 0x%lx %lu B %lu MB\n",
        (int)it.first, it.second, it.second, it.second / 1024 / 1024);
  }
  for (auto& it : hdd_level_2_write_bytes) {
    Debug(logger, "hdd write bytes level %d 0x%lx %lu B %lu MB\n",
        (int)it.first, it.second, it.second, it.second / 1024 / 1024);
  }
}

void ZenStat::PrintStat(double time) {
  fprintf(stderr, "(stat) time %.3lf tot %ld bytes %.3lf GiB cnt %ld or %.3lf M\n", 
      time, total_rwb, (double)total_rwb / (1ull << 30), 
      total_rwc, (double)total_rwc / 1000000);
  fprintf(stderr, "(stat) time %.3lf r %ld bytes %.3lf GiB cnt %ld or %.3lf M\n", 
      time, total_rb(), (double)total_rb() / (1ull << 30),
      total_rc(), (double)total_rc() / 1000000);
  fprintf(stderr, "(stat) time %.3lf w %ld bytes %.3lf GiB cnt %ld or %.3lf M\n", 
      time, total_wb(), (double)total_wb() / (1ull << 30),
      total_wc(), (double)total_wc() / 1000000);
  fprintf(stderr, "(stat) time %.3lf cr %ld bytes %.3lf GiB cnt %ld or %.3lf M\n", 
      time, total_crb(), (double)total_crb() / (1ull << 30),
      total_crc(), (double)total_crc() / 1000000);
  fprintf(stderr, "(stat) time %.3lf wa %ld bytes %.3lf GiB cnt %ld or %.3lf M\n", 
      time, total_wab(), (double)total_wab() / (1ull << 30),
      total_wac(), (double)total_wac() / 1000000);
}

WorkloadStatus ZenStat::status() {
  return ws;
}

bool ZenStat::printable() {
  bool ret = false;
  stat_mtx_.lock();
  if (printable_) {
    ret = true;
    printable_ = false;
  }
  stat_mtx_.unlock();

  return ret; 
}

uint64_t ZenStat::total_rc() { return total(total_cnt, {0, 1, 4, 5}); }
uint64_t ZenStat::total_wc() { return total(total_cnt, {2, 3, 6, 7}); }
uint64_t ZenStat::total_rb() { return total(total_bytes, {0, 1, 4, 5}); }
uint64_t ZenStat::total_wb() { return total(total_bytes, {2, 3, 6, 7}); }
uint64_t ZenStat::total_crc() { return total(total_cnt, {1, 5}); }
uint64_t ZenStat::total_crb() { return total(total_bytes, {1, 5}); }
uint64_t ZenStat::total_wac() { return total(total_cnt, {3, 7}); }
uint64_t ZenStat::total_wab() { return total(total_bytes, {3, 7}); }

} // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)

