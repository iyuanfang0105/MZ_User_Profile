#!/bin/sh
echo "+++++"
echo "+++++Trainning fastText mode"
echo "+++++"

if [[ "$#" -ne 7 ]]; then
    echo "Usage: ./train.sh train_data_dir fasttext_path model_save_dir epoch ngram train_test_ratio raw_data_hdfs_path"
    exit 1
fi

train_data_dir=$1
fasttext_dir=$2
model_save_dir=$3
epoches=$4
ngram=$5
trian_test_ratio=$6
raw_data_hdfs_path=$7

echo -e "\n====>>>> train data dir: $train_data_dir \n"
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


echo -e "\n====>>>> trainning \n"
model_save=${model_save_dir}/ft.model
train_data=${train_data_dir}/labeled.data.shuf.train
test_data=${train_data_dir}/labeled.data.shuf.test
echo ${train_data}
echo ${model_save}

./${fasttext_dir}/fasttext supervised -input ${train_data} -epoch ${epoches} -wordNgrams ${ngram} -output ${model_save}

echo -e "\n====>>>> testing \n"
cat ${test_data} | cut -d ',' -f2 | ./${fasttext_dir}/fasttext predict ${model_save_dir}/ft.model.bin - | awk -F "," '{print $1}' > ${train_data_dir}/test.result
cat ${test_data} | cut -d ',' -f1 > ${train_data_dir}/test.groundtruth

./result.py ${train_data_dir}/test.groundtruth ${train_data_dir}/test.result
#wc jd.labeled.data.shuf > xxx
#read lines words characters filename < xxx
#printf "%.0f" "$lines*0.8"
