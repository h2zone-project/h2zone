#!/bin/bash

SCRIPTS_DIR=${PWD}
ROCKSDB_DIR="${PWD}/../workspace/rocksdb/"
RESULTS_DIR="${PWD}/../workspace/results/"
YCSB_DIR="${PWD}/../workspace/ycsb/"

option_file_suffix=$1
result_dir_suffix=$3
update_is_rmw=$4  # 1 or 0
zipf=$5    # zipfian constant
name_suffix=$6

## Sanitize
option_file="${ROCKSDB_DIR}/plugin/zenfs/options_${option_file_suffix}"

result_dir="${RESULTS_DIR}/${result_dir_suffix}"
if [[ ! -d $result_dir ]]; then
  mkdir -p $result_dir
  sudo chmod 777 $result_dir
fi

list="a"
if [[ $# -ge 7 ]]; then
  list="$7"
fi

########################
# Load
config_file="${SCRIPTS_DIR}/config.sh"

if [[ "${list}" == "load" ]]; then
  ops="100m"
  if [[ $# -ge 8 ]]; then
    ops=${8}
  fi
  name_suffix=${name_suffix}_${ops}
  ops=`echo $ops | sed 's/k/000/g' | sed 's/m/000000/g'`
  echo "recordcount=$ops" >> $config_file
  echo "insertcount=$ops" >> $config_file

  ${SCRIPTS_DIR}/single_exp.sh $option_file $update_is_rmw $zipf "load" $config_file \
     ${YCSB_DIR}/workloads/workloada ${result_dir}/load_${name_suffix}
  exit
fi

########################
# Normal workload

if [[ $# -ge 9 ]]; then
  ops=${9}
  ops=`echo $ops | sed 's/k/000/g' | sed 's/m/000000/g'`
  echo "recordcount=$ops" >> $config_file
  echo "insertcount=$ops" >> $config_file
fi

ops="1m"
if [[ $# -ge 8 ]]; then
  ops=${8}
fi
name_suffix=${name_suffix}_${ops}
ops=`echo $ops | sed 's/k/000/g' | sed 's/m/000000/g'`
echo "operationcount=$ops" >> $config_file

wl="workload${list}"
${SCRIPTS_DIR}/single_exp.sh $option_file $update_is_rmw $zipf "run" $config_file \
  ${YCSB_DIR}/workloads/${wl} ${result_dir}/${list}_${name_suffix}

