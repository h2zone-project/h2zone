zenfs_SOURCES = fs/fzbd_zenfs.cc fs/fs_zenfs.cc fs/zbd_zenfs.cc fs/zone.cc fs/io_zenfs.cc fs/io_compaction_hint.cc fs/io_mig_zenfs.cc fs/io_cache.cc fs/cache_manager.cc fs/util.cc fs/zenfs_options.cc fs/fs_compaction_hint.cc fs/fs_mig_zenfs.cc
zenfs_HEADERS = fs/fs_zenfs.h fs/zbd_zenfs.h fs/io_zenfs.h fs/cache_manager.h fs/util.h fs/zenfs_options.h
zenfs_LDFLAGS = -lzbd -u zenfs_filesystem_reg
