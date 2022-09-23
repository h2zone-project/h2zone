#!/bin/bash

mkdir -p ../workspace
cd ../workspace

##########
echo "0. Please prepare the libzbd.a file at /usr/lib by installing libzbd"

if [[ ! -f "/usr/lib/libzbd.a" ]]; then
  echo "/usr/lib/libzbd.a not found; please install libzbd"
  exit
fi

##########
echo "1. Prepare the code of rocksdb 6.22.1"

if [[ ! -d rocksdb ]]; then
  git clone https://github.com/facebook/rocksdb rocksdb
  cd rocksdb
  git checkout tags/v6.22.1
  cd ..
fi

if [[ ! -f "rocksdb/libzbd.a" ]]; then
  cp /usr/lib/libzbd.a rocksdb/
fi

##########
echo "2. Copy the modified RocksDB source code"

find ../src/rocksdb/ -type f | sed "s,../src/rocksdb/,," | while read line ; do
  src_file="../src/rocksdb/$line"
  dest_file="rocksdb/$line"

  if [[ ! -f ${dest_file} || "`cmp ${src_file} ${dest_file} | wc -l`" == 1 ]]; then
    echo "moving $line"
    cp ${src_file} ${dest_file}
  fi
done

##########
echo "3. Copy the plugin source code"

mkdir -p settings
cp ../etc/zeta_results settings/

find ../src/zenfs/ -type f | sed "s,../src/zenfs/,," | while read line; do
  src_file="../src/zenfs/$line"
  dest_file="rocksdb/plugin/zenfs/$line"

  dir=`dirname $dest_file`

  if [[ ! -d $dir ]]; then
    mkdir -p $dir
  fi

  if [[ ${src_file} != *.swp ]]; then
    if [[ ! -f ${dest_file} || "`cmp ${src_file} ${dest_file} | wc -l`" == 1 ]]; then
      echo "moving $line"
      cp ${src_file} ${dest_file}
    fi
  fi
done

SETTINGS_DIR="${PWD}/settings/"
ROCKSDB_DIR="${PWD}/rocksdb/"

sed -i "s,ROCKSDB_DIR,${ROCKSDB_DIR}," rocksdb/plugin/zenfs/util/Makefile
sed -i "s,ROCKSDB_DIR,${ROCKSDB_DIR}," rocksdb/plugin/zenfs/fs/zenfs_options.cc
sed -i "s,SETTINGS_DIR,${SETTINGS_DIR}," rocksdb/java/rocksjni/rocksjni.cc 

##########
echo "4. Compile"
echo "4.1 Compiling RocksDB library"

cd ${ROCKSDB_DIR}/plugin/zenfs/fs
rm -f *.o
cd ${ROCKSDB_DIR}

sudo rm -rf ${ROCKSDB_DIR}/plugin/zenfs/fs/*.o
sudo DEBUG_LEVEL=0 ROCKSDB_PLUGINS=zenfs make -j4 db_bench install 

if [[ $? -ne 0 ]]; then
  echo "    Compile rocksdb library failed"
  exit
fi

echo "4.2 Compiling ZenFS Util"
cd ${ROCKSDB_DIR}/plugin/zenfs/util
sudo rm zenfs
sudo make

if [[ $? -ne 0 ]]; then
  echo "    Compile ZenFS util failed"
  exit
fi

echo "4.3 Compiling Java release for YCSB"
cd ${ROCKSDB_DIR}

if [[ "${JAVA_HOME}" == "" || ! -d ${JAVA_HOME} ]]; then
  echo "Error: Please configure your JAVA_HOME correctly"
  exit
fi

sudo rm -rf ${ROCKSDB_DIR}/jls/plugin/zenfs/fs
sudo -E DEBUG_LEVEL=0 ROCKSDB_PLUGINS=zenfs JAVA_HOME=$JAVA_HOME make -j4 rocksdbjavastaticrelease
sudo chmod 766 ${ROCKSDB_DIR}/java/target/rocksdbjni-6.22.1.jar

if [[ $? -ne 0 ]]; then
  echo "    Compile Java release of YCSB failed"
  exit
fi
