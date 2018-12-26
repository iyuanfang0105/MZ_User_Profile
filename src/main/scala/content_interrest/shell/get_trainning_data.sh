#!/bin/sh
echo "+++++"
echo "+++++Trainning fastText mode"
echo "+++++"

if [[ "$#" -ne 3 ]]; then
    echo "Usage: ./train.sh raw_data_hdfs_path train_data_dir train_test_ratio"
    exit 1
fi

raw_data_hdfs_path=$1
train_data_dir=$2
trian_test_ratio=$3

echo -e "\n====>>>> Getting train data from: ${raw_data_hdfs_path}, tmp save to: ${train_data_dir}\n"
hadoop fs -text ${raw_data_hdfs_path}/* > ${train_data_dir}/labeled.data
shuf ${train_data_dir}/labeled.data > ${train_data_dir}/labeled.data.shuf

lines=($(wc -l ${train_data_dir}/labeled.data.shuf))
echo -e "\n====>>>> lines count: $lines \n"

train_set_ratio=${trian_test_ratio}
echo -e "\n====>>>> train set ratio: $train_set_ratio \n"

lp=$(echo "$lines*$train_set_ratio/1" | bc)
lp_1=$(echo "$lp+1" | bc)
echo -e "\n====>>>> split point_0: $lp, point_1: $lp_1 \n"

sed -n 1,${lp}p ${train_data_dir}/labeled.data.shuf > ${train_data_dir}/labeled.data.shuf.train
sed -n ${lp_1},${lines}p ${train_data_dir}/labeled.data.shuf > ${train_data_dir}/labeled.data.shuf.test
wc -l ${train_data_dir}/*

