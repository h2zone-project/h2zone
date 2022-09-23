#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/time.h>

#include <string>
#include <cstring>
#include <cstdio>

void func(std::string filename) {
  zbd_info info;
  int read_f_ = zbd_open(filename.c_str(), O_RDONLY, &info);
  int write_f_ = zbd_open(filename.c_str(), O_WRONLY | O_DIRECT | O_EXCL, &info);
  struct zbd_zone *zone_rep;

  printf("%s:\nblock_sz = %d, zone_sz = %lld, nr_zones = %d\n", 
      filename.c_str(),
      info.pblock_size, info.zone_size, info.nr_zones);

  uint64_t addr_space_sz = info.zone_size * info.nr_zones;
  uint32_t reported_zones;
  int ret = zbd_list_zones(read_f_, 0, addr_space_sz, ZBD_RO_ALL, &zone_rep,
      &reported_zones);

  // stat
  timeval tv1, tv2;
  gettimeofday(&tv1, nullptr);
  uint64_t reset_bytes = 0;

  reported_zones = (reported_zones >= 2000) ? 2000 : reported_zones;

  for (int i = 0; i < (int)reported_zones; i++) 
  {
    struct zbd_zone *z = &zone_rep[i];
    if (i == 0) {
      printf("zone capacity: %llu MiB\n", z->capacity / 1024 / 1024);
    }
    if (zbd_zone_type(z) != ZBD_ZONE_TYPE_SWR) {
      continue;
    }

    uint64_t start = zbd_zone_start(z);

    if (zbd_zone_cond(z) == 1) {
      continue;
    }
    printf("Start to reset zone %d\n", i);

    ret = zbd_reset_zones(write_f_, start, info.zone_size); 

    if (ret) {
      printf("Error: Zone %d reset failed, errno %d, %s, cond %d\n", 
          i, errno, strerror(errno), zbd_zone_cond(z));
      exit(1);
    }

    reset_bytes += info.zone_size;
  }

  gettimeofday(&tv2, nullptr);
  double t = (double)tv2.tv_sec - tv1.tv_sec + tv2.tv_usec / 1000000.0 
    - tv1.tv_usec / 1000000.0;
  printf("Reset finished, bytes = 0x%lx MiB, "
      "time = %.5lf s, spd = %.5lf MiB/s\n", 
      reset_bytes / 1024 / 1024, t, reset_bytes / 1024 / 1024 / t);
}

int main(int argc, char** argv) {
  char username[200];
  cuserid(username);

  sleep(2);

  if (strcmp(username, "root")) {
    printf("Run with root!\n");
    return 1;
  }

  if (argc < 2) {
    printf("Put the device name!\n");
    return 1;
  }

  func(argv[1]);

  return 0;
}
