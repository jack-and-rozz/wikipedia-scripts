#!/bin/bash
usage() {
    #echo "Usage:$0 target_dir max_items max_properties"
    echo "Usage:$0 target_dir"
    exit 1
}

if [ $argc -lt 1 ];then
    usage;
fi
target_dir=$1
#max_items=$2
#max_properties=$3

# extract QRQ-triples and the information of items, properties 
python extract_all.py $target_dir --cleanup=F >> $target_dir/extract_all.log 
python extract_subgraph.py $target_dir >> $target_dir/extract_subgraph.log 
