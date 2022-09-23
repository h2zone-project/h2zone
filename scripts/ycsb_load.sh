#!/bin/bash

source $1 

##################
echo "Check and reset devices..."

if [[ "$DEV" == "" ]]; then
  echo "Do not find any devices. Please set the DEV variable manually in config.sh"
  exit
fi

echo "$DEV" | awk -F '_' '{for (i=1; i<=NF; i++) print $i;}' | while read line; do
  if [[ ! -e "/dev/${line}" ]]; then
    echo "Device ${line} not exist. Please check your config.sh"
    exit
  fi

  if [[ ! -f "zbd/reset" ]]; then
    cd zbd
    ./compile.sh
    cd ..
  fi
  sudo zbd/reset /dev/${line}

  n2=`cat /sys/block/${line}/queue/scheduler | grep "\[mq-deadline\]" | wc -l`
  if [[ $n2 -ne 1 ]]; then
    echo "scheduler error!"
    echo "Hint: You can run: sudo bash -c 'echo \"mq-deadline\" > /sys/block/${line}/queue/scheduler' to set the scheduler"
    exit
  fi
done


##################
echo "Restart and format ZenFS ..."
SCRIPTS_DIR=${PWD}
ROCKSDB_DIR="${PWD}/../workspace/rocksdb/"
SETTINGS_DIR="${PWD}/../workspace/settings/"
YCSB_DIR="${PWD}/../workspace/ycsb/"

cd ${ROCKSDB_DIR}/plugin/zenfs/util 
aux_path=/tmp/zenfs_ycsb
sudo rm -rf $aux_path
sudo ./zenfs mkfs --zbd=$DEV --aux-path=$aux_path --finish_threshold=7 --force
cd ${SCRIPTS_DIR}


##################
echo "Start loading on $DEV ..."

OUTPUT=$2 
WORKLOAD=$3 
PATH_YCSB=/ycsb-rocksdb-data

echo "zenfs://dev:$DEV" > ${SETTINGS_DIR}/jni_zenfs_url
sudo rm -rf $PATH_YCSB

cd ${YCSB_DIR} 

sudo ./bin/ycsb load rocksdb -target $target -s -P $WORKLOAD -p rocksdb.dir=$PATH_YCSB \
    -p recordcount=$recordcount -p insertcount=$insertcount \
    -p hdrhistogram.percentiles=10,25,50,75,90,95,99,99.9,99.99 \
    -p operationcount=$operationcount 2>&1 | tee $OUTPUT.out  

echo ${OUTPUT}.out
sudo chmod 666 ${OUTPUT}.out

file="`grep -r "Created l" ${OUTPUT}.out | awk '{print $5;}' | awk -F ',' '{print $1;}'`"
if [[ -f $file ]]; then
  echo "copying file $file"
  sudo cp $file ${OUTPUT}.log
  sudo chmod 666 $file
  sudo chmod 666 ${OUTPUT}.log
fi
