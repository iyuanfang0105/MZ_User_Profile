#!/bin/sh
echo '+++++'
echo '+++++ infer unlabelled data'
echo '+++++'



echo -e "\n====>>>> getting unlabelled_data of mzreader"

cd ~/up/fasttext/data/content_interrest/unlabelled_data
rm -rf *

hadoop fs -text /apps/recommend/models/wind/content_interrest/first_level/unlabeled_data/mzreader_title/* > mzreader.title
hadoop fs -text /apps/recommend/models/wind/content_interrest/first_level/unlabeled_data/mzreader_content/* > mzreader.content

hadoop fs -text /apps/recommend/models/wind/content_interrest/first_level/unlabeled_data/browser_3p_title/* > browser.3p.title
hadoop fs -text /apps/recommend/models/wind/content_interrest/first_level/unlabeled_data/browser_3p_content/* > browser.3p.content

hadoop fs -text /apps/recommend/models/wind/content_interrest/first_level/unlabeled_data/notice_title/* > notice.title
hadoop fs -text /apps/recommend/models/wind/content_interrest/first_level/unlabeled_data/notice_content/* > notice.content


cat mzreader.title | awk -F "##yf##" '{print $1}' > mzreader.title.fid
cat mzreader.title | awk -F "##yf##" '{print $2}' | ./../../../fastText-0.1.0/fasttext predict-prob ../../../model/content_interrest/title_model/ft.model.bin - 1 > mzreader.title.predict
paste -d '|' mzreader.title.fid mzreader.title.predict > mzreader.title.result

cat browser.3p.title | awk -F "##yf##" '{print $1}' > browser.3p.title.fid
cat browser.3p.title | awk -F "##yf##" '{print $2}' | ./../../../fastText-0.1.0/fasttext predict-prob ../../../model/content_interrest/title_model/ft.model.bin - 1 > browser.3p.title.predict
paste -d '|' browser.3p.title.fid browser.3p.title.predict > browser.3p.title.result

cat notice.title | awk -F "##yf##" '{print $1}' > notice.title.fid
cat notice.title | awk -F "##yf##" '{print $2}' | ./../../../fastText-0.1.0/fasttext predict-prob ../../../model/content_interrest/title_model/ft.model.bin - 1 > notice.title.predict
paste -d '|' notice.title.fid notice.title.predict > notice.title.result



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
