# h2zone
The H2Zone prototype for hybrid zoned storage.

## Build

### Testbed requirement

1. Libraries or tools: git, Rscript, libzbd
2. A ZNS SSD and an HM-SMR HDD. If you don't have real devices, you can consider using the QEMU to emulate either the SSD or the HDD, or both of them. You can refer to the zoned storage website: 
  + https://zonedstorage.io/docs/getting-started/smr-emulation
  + https://zonedstorage.io/docs/getting-started/zns-emulation 
3. Compiler tools: g++, make
4. Kernel version: higher than 4.10.0, to support `blkzoned.h`

### Build libraries or tools 

libzbd. Run the commands, and check whether `libzbd.a` is in `/usr/lib/`.  Note that it requires `autoconf`, `autoconf-archive`, `automake`, `libtool`, and `m4`.  

```
$ git clone https://github.com/westerndigitalcorporation/libzbd
$ cd libzbd
$ sh ./autogen.sh
$ ./configure
$ make
$ cd ..
```

git:

```
$ sudo apt-get install git
```

R:
```
$ sudo apt-get install r-base-core
```

### Integrate with RocksDB and ZenFS

```
$ cd scripts
$ ./install_rocksdb_zenfs.sh  
$ cd .. 
```

### Install YCSB

```
$ cd scripts
$ ./install_ycsb.sh
$ cd ..
```

You can also run `install.sh` directly to install both of them.

## Run the Demo

Before you run the demo, you should prepare your zoned devices. For example, if your ZNS SSD is `/dev/nvme0n1`, and your HM-SMR HDD is `/dev/sdc`, put the following line in the `scripts/config.sh` file. The SSD should always appear before the HDD.

```
DEV="nvme0n1_sdc"
```

Then you should check whether both of your devices are set to "mq-deadline" in the scheduler. For example, use the following command to check the scheduler of `/dev/nvme0n1` (It shows 1 if succeeds, or 0 if fails):

```
$ cat /sys/block/nvme0n1/queue/scheduler | grep "\[mq-deadline\]" | wc -l
```

If the command above puts zero, use the following command to set the scheduler of `/dev/nvme0n1`:

```
sudo bash -c 'echo \"mq-deadline\" > /sys/block/nvme0n1/queue/scheduler'
```

After that, you can run the following script to directly run a demo of load and workload A in YCSB. 

```
$ cd scripts
$ ./run_demo.sh
$ cd ..
