#!/bin/sh
echo '+++++'
echo '+++++ infer unlabelled data'
echo '+++++'

time=$4
date=${time:0:8}

echo -e "\n====>>>> getting unlabelled_data of ${date}\n"

cd ~/up/fastText/data/content_interrest/unlabelled_data/
#rm -rf *

pwd

hadoop fs -text /apps/recommend/models/wind/content_interrest/unlabeled_data/mzreader_title/${date}/* > mzreader.title
# hadoop fs -text /apps/recommend/models/wind/content_interrest/first_level/unlabeled_data/mzreader_content/* > mzreader.content

hadoop fs -text /apps/recommend/models/wind/content_interrest/unlabeled_data/browser_3p_title/${date}/* > browser.3p.title
# hadoop fs -text /apps/recommend/models/wind/content_interrest/first_level/unlabeled_data/browser_3p_content/* > browser.3p.content

# hadoop fs -text /apps/recommend/models/wind/content_interrest/first_level/unlabeled_data/notice_title/* > notice.title
hadoop fs -text /apps/recommend/models/wind/content_interrest/unlabeled_data/notice_content/${date}/* > notice.content
wc -l *

cat mzreader.title | awk -F "##yf##" '{print $1}' > mzreader.title.fid
cat mzreader.title | awk -F "##yf##" '{print $2}' | ./../../../fastText-0.1.0/fasttext predict-prob ../../../model/content_interrest/title_model/first_level/ft.model.bin - 1 > mzreader.title.predict.first.level
cat mzreader.title | awk -F "##yf##" '{print $2}' | ./../../../fastText-0.1.0/fasttext predict-prob ../../../model/content_interrest/title_model/second_level/ft.model.bin - 1 > mzreader.title.predict.second.level

paste -d '|' mzreader.title.fid mzreader.title.predict.first.level mzreader.title.predict.second.level > mzreader.title.result
head -1 mzreader.title.result

cat browser.3p.title | awk -F "##yf##" '{print $1}' > browser.3p.title.fid
cat browser.3p.title | awk -F "##yf##" '{print $2}' | ./../../../fastText-0.1.0/fasttext predict-prob ../../../model/content_interrest/title_model/first_level/ft.model.bin - 1 > browser.3p.title.predict.first.level
cat browser.3p.title | awk -F "##yf##" '{print $2}' | ./../../../fastText-0.1.0/fasttext predict-prob ../../../model/content_interrest/title_model/second_level/ft.model.bin - 1 > browser.3p.title.predict.second.level

paste -d '|' browser.3p.title.fid browser.3p.title.predict.first.level browser.3p.title.predict.second.level > browser.3p.title.result
head -1 browser.3p.title.result

cat notice.content | awk -F "##yf##" '{print $1}' > notice.content.fid
cat notice.content | awk -F "##yf##" '{print $2}' | ./../../../fastText-0.1.0/fasttext predict-prob ../../../model/content_interrest/title_model/first_level/ft.model.bin - 1 > notice.content.predict.first.level
cat notice.content | awk -F "##yf##" '{print $2}' | ./../../../fastText-0.1.0/fasttext predict-prob ../../../model/content_interrest/title_model/second_level/ft.model.bin - 1 > notice.content.predict.second.level

paste -d '|' notice.content.fid notice.content.predict.first.level notice.content.predict.second.level > notice.content.result
head -1 notice.content.result


wc -l *

hadoop fs -put -f mzreader.title.result /apps/recommend/models/wind/content_interrest/unlabeled_data/mzreader_title/${date}/mzreader.title.result
hadoop fs -put -f browser.3p.title.result /apps/recommend/models/wind/content_interrest/unlabeled_data/browser_3p_title/${date}/browser.3p.title.result
hadoop fs -put -f notice.content.result /apps/recommend/models/wind/content_interrest/unlabeled_data/notice_content/${date}/notice.content.result


# if [ "$#" -ne 1 ]; then
#     echo "Usage: ./get_infer_data.sh save_dir"
#     exit 1
# else
#     data_save_dir=$1
#     echo -e "\n====>>>> data_save_dir: $data_save_dir"
# fi

# echo -e "\n====>>>> Get infer data from hdfs"
# rm -rf $data_save_dir/*

# echo -e "\n====>>>> the data of third party commerce"
# hadoop fs -text /apps/recommend/models/wind/shopping_interrest/third_party_commerce/* > $data_save_dir/third.party.commerce

# cat $data_save_dir/third.party.commerce | awk -F "##yf##" '{print $1}' > $data_save_dir/third.party.commerce.fmd5
# cat $data_save_dir/third.party.commerce | awk -F "##yf##" '{print $2}' > $data_save_dir/third.party.commerce.title

# echo -e "\n====>>>> the data of notification"
# hadoop fs -text /apps/recommend/models/wind/shopping_interrest/notification/* > $data_save_dir/notification

# cat $data_save_dir/notification | awk -F "##yf##" '{print $1}' > $data_save_dir/notification.imei
# cat $data_save_dir/notification | awk -F "##yf##" '{print $2}' > $data_save_dir/notification.content

# echo -e "\n====>>>> the data of browser"
# hadoop fs -text /apps/recommend/models/wind/shopping_interrest/browser/* > $data_save_dir/browser

# cat $data_save_dir/browser | awk -F "##yf##" '{print $1}' > $data_save_dir/browser.imei
# cat $data_save_dir/browser | awk -F "##yf##" '{print $2}' > $data_save_dir/browser.content


# wc -l $data_save_dir/*
# head -1 $data_save_dir/third.party.commerce
# head -1 $data_save_dir/notification
# head -1 $data_save_dir/browser
