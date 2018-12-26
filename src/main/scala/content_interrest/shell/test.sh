echo -e "\n====>>>> Inferring notification data \n"
echo -e "\n====>>>> first level"
cat ../data/infer/notification.content | ./../fastText-0.1.0/fasttext predict ../model/first_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/notification.content.first.level.pred

echo -e "====>>>> second level"
cat ../data/infer/notification.content | ./../fastText-0.1.0/fasttext predict ../model/second_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/notification.content.second.level.pred

echo -e "====>>>> third level"
cat ../data/infer/notification.content | ./../fastText-0.1.0/fasttext predict ../model/third_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/notification.content.third.level.pred

echo -e "====>>>> generate final result of notification"
cd ../data/infer
paste -d '|' notification.imei notification.content notification.content.first.level.pred > notification.first.level.result
paste -d '|' notification.imei notification.content notification.content.second.level.pred > notification.second.level.result
paste -d '|' notification.imei notification.content notification.content.third.level.pred > notification.third.level.result

wc -l notification.*.level.result

hadoop fs -put -f notification.*.level.result /apps/recommend/models/wind/shopping_interrest/result/