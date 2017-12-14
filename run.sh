#!/bin/bash
usage() {
    echo "Usage:$0 target_dir"
    exit 1
}


if [ $# -lt 1 ];then
    usage;
fi

python wp.combine_wd.py --min_qfreq=5 --target_dir=$1 > $1/q5.log
python wp.combine_wd.py --min_qfreq=10 --target_dir=$1 > $1/q10.log
python wp.combine_wd.py --min_qfreq=20 --target_dir=$1 > $1/q20.log
python wp.combine_wd.py --min_qfreq=50 --target_dir=$1 > $1/q50.log
python wp.combine_wd.py --min_qfreq=100 --target_dir=$1 > $1/q100.log 
tail -n200 $1/q*.log > $1/combine_wd.log
