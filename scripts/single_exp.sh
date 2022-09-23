#!/bin/bash

generate_file_name() {
  i=0
  file=$1
  filename="${file}_try${i}"

  while [[ $i -lt 100 ]]; do
    filename="${file}_try${i}"
    if [[ -f "$filename.out" || -f "$filename" ]]; then
      i=$(($i+1))
      continue
    fi
    break
  done

  echo "$filename"
}

SCRIPTS_DIR=${PWD}
ROCKSDB_DIR="${PWD}/../workspace/rocksdb/"
SETTINGS_DIR="${PWD}/../workspace/settings/"
YCSB_DIR="${PWD}/../workspace/ycsb/"

option_file=$1    # config file
update_is_rmw=$2  # 1 or 0
zipf=$3           # zipfian constant
phase=$4          # load or run
config_file=$5
workload=$6
result_file=$7

echo "Check the zipfian factor ..."

zeta=`awk 'BEGIN {zeta=0;} {if ($1=='"$zipf"') zeta=$2;} END {print zeta;}' ${SETTINGS_DIR}/zeta_results`
if [[ "$zeta" == "0" ]]; then
  echo "no such zeta value for zipfian $zipf"
  exit
  zeta=`awk 'BEGIN {zeta=0;} {if ($1==0.99) zeta=$2;} END {print zeta;}' ${SETTINGS_DIR}/zeta_results`
fi

result_file=`generate_file_name "$result_file"`

cp $option_file ${ROCKSDB_DIR}/plugin/zenfs/options.txt
echo "$zipf" > ${SETTINGS_DIR}/zipf
echo "$zeta" > ${SETTINGS_DIR}/zeta
echo "$update_is_rmw" > ${SETTINGS_DIR}/updateIsRmw

source $config_file

need_gen=`echo "$zipf" | awk '{if ($1>=1) print 1; else print 0;}'`
if [[ "$need_gen" == "1" ]]; then
  Rscript ${SCRIPTS_DIR}/gen.r $zipf $recordcount $operationcount 
  mv out.data ${SETTINGS_DIR}/pattern.data
fi

if [[ "$phase" == "load" ]]; then
  ${SCRIPTS_DIR}/ycsb_load.sh $config_file $result_file $workload 
elif [[ "$phase" == "run" ]]; then
  ${SCRIPTS_DIR}/ycsb_run.sh $config_file $result_file $workload 
fi
