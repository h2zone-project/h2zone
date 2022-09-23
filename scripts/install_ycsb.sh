#!/bin/bash

mkdir -p workspace
cd ../workspace
SETTINGS_DIR="${PWD}/settings/"
ROCKSDB_DIR="${PWD}/rocksdb/"
USER="" ## modify here (the username you used for running ycsb)

##########

echo "5. Prepare the code of YCSB"

if [[ ! -d ycsb ]]; then
  git clone https://github.com/brianfrankcooper/YCSB.git ycsb
  cd ycsb
  git reset --hard ce3eb9ce51c8
  mvn -pl site.ycsb:rocksdb-binding -am clean package
  cd ..
fi

##########

echo "6. Copy the modified YCSB source code"

find ../src/ycsb/ -type f | sed "s,../src/ycsb/,," | while read line ; do
  src_file="../src/ycsb/$line"
  dest_file="ycsb/$line"

  if [[ ! -f ${dest_file} || "`cmp ${src_file} ${dest_file} | wc -l`" == 1 ]]; then
    echo "moving $line"
    cp ${src_file} ${dest_file}
  fi
done

sed -i "s,SETTINGS_DIR,${SETTINGS_DIR}," ycsb/core/src/main/java/site/ycsb/generator/ReadFromFileGenerator.java 
sed -i "s,SETTINGS_DIR,${SETTINGS_DIR}," ycsb/core/src/main/java/site/ycsb/workloads/CoreWorkload.java 
sed -i "s,SETTINGS_DIR,${SETTINGS_DIR}," ycsb/core/src/main/java/site/ycsb/workloads/TraceReader.java 
sed -i "s,SETTINGS_DIR,${SETTINGS_DIR}," ycsb/rocksdb/src/main/java/site/ycsb/db/rocksdb/RocksDBClient.java 

cd ycsb
sudo mvn -pl site.ycsb:rocksdb-binding -am clean package -Dmaven.test.skip
cd ..

##########

echo "7. Copy the modified RocksDB jar"

#### Check localRepository setting
sudo grep "localRepository" /root/.m2/settings.xml > /dev/null
if [[ $? -ne 0 ]]; then
  echo "Error: Please check localRepository settings in /root/.m2/settings.xml"
  exit
fi

PATH_JAR="${ROCKSDB_DIR}/java/target/rocksdbjni-6.22.1.jar"
sudo cp ${PATH_JAR} /home/$USER/.m2/repository/org/rocksdb/rocksdbjni/6.22.1/ 
