#!/bin/bash

set -x

#g++ check_zones.cc -o check_zones -std=c++11 -lzbd -Wall
#g++ write_all_zones.cc -o write_zones -std=c++11 -lzbd -Wall
#g++ read_all_zones.cc -o read_zones -std=c++11 -lzbd -Wall
sudo g++ reset_all_zones.cc -o reset -std=c++11 -lzbd -Wall
#g++ copy_zones.cc -o copy -std=c++11 -lzbd -Wall
#g++ randwrite_all_zones.cc -o randw -std=c++11 -lzbd -Wall
#g++ randread_all_zones.cc -o randr -std=c++11 -lzbd -lpthread -Wall
#g++ copy_zones_thread.cc -o copy_thread -std=c++11 -lzbd -Wall -lpthread
