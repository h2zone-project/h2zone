#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "rocksdb/env.h"

#include "zenfs_options.h"

namespace ROCKSDB_NAMESPACE {

void ZenFSOptions::Print() {
  fprintf(stderr, "--------------\n");
  fprintf(stderr, "enable_spandb_data_placement %d\n", (int)enable_spandb_data_placement);
  fprintf(stderr, "enable_mutant_data_placement %d\n", (int)enable_mutant_data_placement);
  fprintf(stderr, "enable_data_placement %d\n", (int)enable_data_placement);
  fprintf(stderr, "enable_trivial_data_placement %d\n", (int)enable_trivial_data_placement);
  fprintf(stderr, "enable_explicit_migration %d\n", (int)enable_explicit_migration);
  fprintf(stderr, "enable_cache_manager %d\n", (int)enable_cache_manager);
  fprintf(stderr, "enable_capacity_calibration %d\n", (int)enable_capacity_calibration);
  fprintf(stderr, "enable_aggressive_deletion %d\n", (int)enable_aggressive_deletion);
  fprintf(stderr, "print_zraf_read %d\n", (int)print_zraf_read);
  fprintf(stderr, "print_zwf_write %d\n", (int)print_zwf_write);
  fprintf(stderr, "print_cache %d\n", (int)print_cache);
  fprintf(stderr, "print_window %d\n", (int)print_window);
  fprintf(stderr, "print_migration %d\n", (int)print_migration);
  fprintf(stderr, "log_zwf_write %d\n", (int)log_zwf_write);
  fprintf(stderr, "log_zsf_read %d\n", (int)log_zsf_read);
  fprintf(stderr, "log_zraf_read %d\n", (int)log_zraf_read);
  fprintf(stderr, "log_zf_pushextent %d\n", (int)log_zf_pushextent);
  fprintf(stderr, "kv_sep %d\n", (int)kv_sep);
  fprintf(stderr, "log_zf_read_hdd %d\n", (int)log_zf_read_hdd);
  fprintf(stderr, "log_zf_read %d\n", (int)log_zf_read);
  fprintf(stderr, "enable_no_hinted_caching %d\n", (int)enable_no_hinted_caching);
  fprintf(stderr, "constrained_ssd_space %ld\n", constrained_ssd_space);
  fprintf(stderr, "constrained_hdd_space %ld\n", constrained_hdd_space);
  fprintf(stderr, "max_wal_size %ld\n", max_wal_size);
  fprintf(stderr, "level_m %d\n", level_m);
  fprintf(stderr, "sst_size %ld\n", sst_size);
  fprintf(stderr, "ssd_write_bound %ld\n", ssd_write_bound);
  fprintf(stderr, "hdd_write_bound %ld\n", hdd_write_bound);
  fprintf(stderr, "dynamic_throughput_factor %.1lf\n", dynamic_throughput_factor);
  fprintf(stderr, "hdd_randread_bound %ld\n", hdd_randread_bound);
  fprintf(stderr, "buffer_size_in_block %ld\n", buffer_size_in_block);
  fprintf(stderr, "explicit_throughput %d\n", (int)explicit_throughput);
  fprintf(stderr, "level_capacity_impl %d\n", (int)level_capacity_impl);
  fprintf(stderr, "test_cache_correctness %d\n", (int)test_cache_correctness);
  fprintf(stderr, "print_posix_rw %d\n", (int)print_posix_rw);
  fprintf(stderr, "posix_file_path \"%s\"\n", posix_file_path.c_str());
  fprintf(stderr, "posix_device \"%s\"\n", posix_device.c_str());
  fprintf(stderr, "random_read_test %d\n", (int)random_read_test);
  fprintf(stderr, "--------------\n");
}

void ZenFSOptions::Read() {
  static bool first = true;

  FILE *f = nullptr;
  std::string zenfs_options_file = 
    "ROCKSDB_DIR/plugin/zenfs/options.txt";
  f = fopen(zenfs_options_file.c_str(), "r");
  if (f != nullptr) {
    char token1[200], token2[200];
    while (fscanf(f, "%s %s", token1, token2) != EOF) {
      if (strcmp(token1, "enable_data_placement") == 0) {
        if (strcmp(token2, "true") == 0) {
          enable_data_placement = true;
        } else if (strcmp(token2, "false") == 0) {
          enable_data_placement = false;
        }
      }

      if (strcmp(token1, "enable_spandb_data_placement") == 0) {
        if (strcmp(token2, "true") == 0) {
          enable_spandb_data_placement = true;
        } else if (strcmp(token2, "false") == 0) {
          enable_spandb_data_placement = false;
        }
      }

      if (strcmp(token1, "enable_mutant_data_placement") == 0) {
        if (strcmp(token2, "true") == 0) {
          enable_mutant_data_placement = true;
        } else if (strcmp(token2, "false") == 0) {
          enable_mutant_data_placement = false;
        }
      }

      if (strcmp(token1, "enable_trivial_data_placement") == 0) {
        if (strcmp(token2, "true") == 0) {
          enable_trivial_data_placement = true;
        } else if (strcmp(token2, "false") == 0) {
          enable_trivial_data_placement = false;
        }
      }

      if (strcmp(token1, "print_zraf_read") == 0) {
        if (strcmp(token2, "true") == 0) {
          print_zraf_read = true;
        } else if (strcmp(token2, "false") == 0) {
          print_zraf_read = false;
        }
      }

      if (strcmp(token1, "print_zwf_write") == 0) {
        if (strcmp(token2, "true") == 0) {
          print_zwf_write = true;
        } else if (strcmp(token2, "false") == 0) {
          print_zwf_write = false;
        }
      }

      if (strcmp(token1, "print_window") == 0) {
        if (strcmp(token2, "true") == 0) {
          print_window = true;
        } else if (strcmp(token2, "false") == 0) {
          print_window = false;
        }
      }

      if (strcmp(token1, "print_migration") == 0) {
        if (strcmp(token2, "true") == 0) {
          print_migration = true;
        } else if (strcmp(token2, "false") == 0) {
          print_migration = false;
        }
      }

      if (strcmp(token1, "log_zwf_write") == 0) {
        if (strcmp(token2, "true") == 0) {
          log_zwf_write = true;
        } else if (strcmp(token2, "false") == 0) {
          log_zwf_write = false;
        }
      }

      if (strcmp(token1, "log_zsf_read") == 0) {
        if (strcmp(token2, "true") == 0) {
          log_zsf_read = true;
        } else if (strcmp(token2, "false") == 0) {
          log_zsf_read = false;
        }
      }

      if (strcmp(token1, "log_zraf_read") == 0) {
        if (strcmp(token2, "true") == 0) {
          log_zraf_read = true;
        } else if (strcmp(token2, "false") == 0) {
          log_zraf_read = false;
        }
      }

      if (strcmp(token1, "log_zf_pushextent") == 0) {
        if (strcmp(token2, "true") == 0) {
          log_zf_pushextent = true;
        } else if (strcmp(token2, "false") == 0) {
          log_zf_pushextent = false;
        }
      }

      if (strcmp(token1, "log_zf_read_hdd") == 0) {
        if (strcmp(token2, "true") == 0) {
          log_zf_read_hdd = true;
        } else if (strcmp(token2, "false") == 0) {
          log_zf_read_hdd = false;
        }
      }

      if (strcmp(token1, "log_zf_read") == 0) {
        if (strcmp(token2, "true") == 0) {
          log_zf_read = true;
        } else if (strcmp(token2, "false") == 0) {
          log_zf_read = false;
        }
      }

      if (strcmp(token1, "random_read_test") == 0) {
        if (strcmp(token2, "true") == 0) {
          random_read_test = true;
        } else if (strcmp(token2, "false") == 0) {
          random_read_test = false;
        }
      }

      if (strcmp(token1, "enable_explicit_migration") == 0) {
        if (strcmp(token2, "true") == 0) {
          enable_explicit_migration = true;
        } else if (strcmp(token2, "false") == 0) {
          enable_explicit_migration = false;
        }
      }

      if (strcmp(token1, "enable_cache_manager") == 0) {
        if (strcmp(token2, "true") == 0) {
          enable_cache_manager = true;
        } else if (strcmp(token2, "false") == 0) {
          enable_cache_manager = false;
        }
      }

      if (strcmp(token1, "enable_capacity_calibration") == 0) {
        if (strcmp(token2, "true") == 0) {
          enable_capacity_calibration = true;
        } else if (strcmp(token2, "false") == 0) {
          enable_capacity_calibration = false;
        }
      }

      if (strcmp(token1, "enable_aggressive_deletion") == 0) {
        if (strcmp(token2, "true") == 0) {
          enable_aggressive_deletion = true;
        } else if (strcmp(token2, "false") == 0) {
          enable_aggressive_deletion = false;
        }
      }

      if (strcmp(token1, "dynamic_throughput_factor") == 0) {
        if (strcmp(token2, "true") == 0) {
          dynamic_throughput_factor = true;
        } else if (strcmp(token2, "false") == 0) {
          dynamic_throughput_factor = false;
        }
      }

      if (strcmp(token1, "print_cache") == 0) {
        if (strcmp(token2, "true") == 0) {
          print_cache = true;
        } else if (strcmp(token2, "false") == 0) {
          print_cache = false;
        }
      }

      if (strcmp(token1, "kv_sep") == 0) {
        if (strcmp(token2, "true") == 0) {
          kv_sep = true;
        } else if (strcmp(token2, "false") == 0) {
          kv_sep = false;
        }
      }

      if (strcmp(token1, "test_cache_correctness") == 0) {
        if (strcmp(token2, "true") == 0) {
          test_cache_correctness = true;
        } else if (strcmp(token2, "false") == 0) {
          test_cache_correctness = false;
        }
      }

      if (strcmp(token1, "print_posix_rw") == 0) {
        if (strcmp(token2, "true") == 0) {
          print_posix_rw = true;
        } else if (strcmp(token2, "false") == 0) {
          print_posix_rw = false;
        }
      }

      if (strcmp(token1, "enable_no_hinted_caching") == 0) {
        if (strcmp(token2, "true") == 0) {
          enable_no_hinted_caching = true;
        } else if (strcmp(token2, "false") == 0) {
          enable_no_hinted_caching = false;
        }
      }

      if (strcmp(token1, "constrained_ssd_space") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          constrained_ssd_space = ld;
        }
      }

      if (strcmp(token1, "constrained_hdd_space") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          constrained_hdd_space = ld;
        }
      }

      if (strcmp(token1, "max_wal_size") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          max_wal_size = ld;
        }
      }

      if (strcmp(token1, "level_m") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          level_m = (int)ld;
        }
      }

      if (strcmp(token1, "sst_size") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          sst_size = ld;
        }
      }

      if (strcmp(token1, "ssd_write_bound") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          ssd_write_bound = ld;
        }
      }

      if (strcmp(token1, "hdd_write_bound") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          hdd_write_bound = ld;
        }
      }

      if (strcmp(token1, "hdd_randread_bound") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          hdd_randread_bound = ld;
        }
      }

      if (strcmp(token1, "buffer_size_in_block") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          buffer_size_in_block = ld;
        }
      }

      if (strcmp(token1, "explicit_throughput") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          explicit_throughput = ld;
        }
      }

      if (strcmp(token1, "level_capacity_impl") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          level_capacity_impl = ld;
        }
      }

      if (strcmp(token1, "posix_file_path") == 0) {
        posix_file_path = std::string(token2);
      }

      if (strcmp(token1, "posix_device") == 0) {
        posix_device = std::string(token2);
      }

      if (strcmp(token1, "posix_space_size") == 0) {
        uint64_t ld = 0;
        if (sscanf(token2, "%ld", &ld) == 1) {
          posix_space_size = ld;
        }
      }
    }

    fclose(f);
  } else {
    fprintf(stderr, "Propertyfile %s not exist.\n", 
        zenfs_options_file.c_str());
  }


  // Sanitize
  if (enable_spandb_data_placement) {
    enable_data_placement = false;
    enable_trivial_data_placement = false;
    enable_mutant_data_placement = false;
  }

  if (enable_mutant_data_placement) {
    enable_data_placement = false;
    enable_trivial_data_placement = false;
    enable_explicit_migration = true;  // Mutant features in migration
    enable_capacity_calibration = false;
  }

  if (enable_data_placement && enable_trivial_data_placement) {
    enable_trivial_data_placement = false;
  }

  if (first) {
    Print();
    first = false;
  } 
}

} // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
