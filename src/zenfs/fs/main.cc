

#include "zenfs_options_3.h"
#include <cstdio>
#include <cstring>

//using namespace ROCKSDB_NAMESPACE;


int main() {
  ZenFSOptions zops;
  fprintf(stderr, "%ld\n", zops.sst_size);
}
