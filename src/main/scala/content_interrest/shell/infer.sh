echo '+++++'
echo '+++++ Infer using trained model (fasttext)'
echo '+++++'

echo -e "\n====>>>> Inferring third party commerce data \n"
echo -e "\n====>>>> first level"
cat ../data/infer/third.party.commerce.title | ./../fastText-0.1.0/fasttext predict ../model/first_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/third.party.commerce.title.first.level.pred
echo -e "====>>>> second level"
cat ../data/infer/third.party.commerce.title | ./../fastText-0.1.0/fasttext predict ../model/second_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/third.party.commerce.title.second.level.pred
echo -e "====>>>> third level"
cat ../data/infer/third.party.commerce.title | ./../fastText-0.1.0/fasttext predict ../model/third_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/third.party.commerce.title.third.level.pred


echo -e "\n====>>>> Inferring notification data \n"
echo -e "\n====>>>> first level"
cat ../data/infer/notification.content | ./../fastText-0.1.0/fasttext predict ../model/first_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/notification.content.first.level.pred
echo -e "====>>>> second level"
cat ../data/infer/notification.content | ./../fastText-0.1.0/fasttext predict ../model/second_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/notification.content.second.level.pred
echo -e "====>>>> third level"
cat ../data/infer/notification.content | ./../fastText-0.1.0/fasttext predict ../model/third_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/notification.content.third.level.pred


echo -e "\n====>>>> Inferring browser data \n"
echo -e "\n====>>>> first level"
cat ../data/infer/browser.content | ./../fastText-0.1.0/fasttext predict ../model/first_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/browser.content.first.level.pred
echo -e "====>>>> second level"
cat ../data/infer/browser.content | ./../fastText-0.1.0/fasttext predict ../model/second_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/browser.content.second.level.pred
echo -e "====>>>> third level"
cat ../data/infer/browser.content | ./../fastText-0.1.0/fasttext predict ../model/third_level/ft.model.bin - | awk -F "," '{print $1}' > ../data/infer/browser.content.third.level.pred


cd ../data/infer
echo -e "====>>>> generate final result of third party commerce"
paste -d '|' third.party.commerce.fmd5 third.party.commerce.title third.party.commerce.title.first.level.pred > third.party.commerce.first.level.result
paste -d '|' third.party.commerce.fmd5 third.party.commerce.title third.party.commerce.title.second.level.pred > third.party.commerce.second.level.result
paste -d '|' third.party.commerce.fmd5 third.party.commerce.title third.party.commerce.title.third.level.pred > third.party.commerce.third.level.result


echo -e "====>>>> generate final result of notification"
paste -d '|' notification.imei notification.content notification.content.first.level.pred > notification.first.level.result
paste -d '|' notification.imei notification.content notification.content.second.level.pred > notification.second.level.result
paste -d '|' notification.imei notification.content notification.content.third.level.pred > notification.third.level.result


echo -e "====>>>> generate final result of browser"
paste -d '|' browser.imei browser.content browser.content.first.level.pred > browser.first.level.result
paste -d '|' browser.imei browser.content browser.content.second.level.pred > browser.second.level.result
paste -d '|' browser.imei browser.content browser.content.third.level.pred > browser.third.level.result


wc -l third.party.commerce.*.level.result
wc -l notification.*.level.result
wc -l browser.*.level.result

hadoop fs -put -f third.party.commerce.*.level.result /apps/recommend/models/wind/shopping_interrest/result/
hadoop fs -put -f notification.*.level.result /apps/recommend/models/wind/shopping_interrest/result/ 
hadoop fs -put -f browser.*.level.result /apps/recommend/models/wind/shopping_interrest/result/
