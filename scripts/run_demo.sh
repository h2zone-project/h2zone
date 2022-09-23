#!/bin/bash

append_option() {
  str=$1
  ROCKSDB_DIR="${PWD}/../workspace/rocksdb/"
  file="${ROCKSDB_DIR}/plugin/zenfs/options_${2}"
  echo "$str" >> $file
}

options=("trivial.txt" "spandb.txt" "dmc.txt")
append_option "level_m 3" "trivial.txt" 

names=("b3" "spandb" "h2zone")
ssd_size=("20")

for ((i=0; i<${#options[@]}; i++)); do
  for ((j=0; j<${#ssd_size[@]}; j++)); do
    ss=${ssd_size[$j]}
    name="${names[$i]}_${ss}g"
    dir="${names[$i]}"

    hdd_size_bytes=$(( (500 - ${ss}) * 1024 * 1024 * 1024 ))
    ssd_size_bytes=$(( ${ss} * 8 * 1024 * 1024 * 1024 ))
    nzones=$(( ${ss} * 4 ))

    append_option "constrained_ssd_space $ssd_size_bytes" "${options[$i]}" 
    append_option "constrained_hdd_space $hdd_size_bytes" "${options[$i]}" 

    ./single_exp_wrapper.sh "${options[$i]}" "0" $dir "1" "0.9" $name "load" "200m"  # Load
    ./single_exp_wrapper.sh "${options[$i]}" "0" "tmp" "1" "0.9" $name "c" "40k"  
    ./single_exp_wrapper.sh "${options[$i]}" "0" $dir "0" "0.9" $name "r50" "1m"       # Workload A
  done
done
