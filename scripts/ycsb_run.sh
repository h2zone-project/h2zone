#!/bin/bash

source $1 

OUTPUT=$2 
WORKLOAD=$3 
PATH_YCSB=/ycsb-rocksdb-data

SCRIPTS_DIR=${PWD}
ROCKSDB_DIR="${PWD}/../workspace/rocksdb/"
SETTINGS_DIR="${PWD}/../workspace/settings/"
YCSB_DIR="${PWD}/../workspace/ycsb/"

echo "Start running workload on $DEV ..."
echo "zenfs://dev:$DEV" > ${SETTINGS_DIR}/jni_zenfs_url

cd ${YCSB_DIR} 

sudo ./bin/ycsb run rocksdb -target $target -s -P $WORKLOAD -p rocksdb.dir=$PATH_YCSB \
    -p recordcount=$recordcount -p insertcount=$insertcount \
    -p hdrhistogram.percentiles=10,25,50,75,90,95,99,99.9,99.99 \
    -p operationcount=$operationcount 2>&1 | tee ${OUTPUT}.out  

echo ${OUTPUT}.out
sudo chmod 666 ${OUTPUT}.out

file="`grep -r "Created l" ${OUTPUT}.out | awk '{print $5;}' | awk -F ',' '{print $1;}'`"
if [[ -f $file ]]; then
  echo "copying file $file"
  sudo cp $file ${OUTPUT}.log
  sudo chmod 666 ${OUTPUT}.log
fi
