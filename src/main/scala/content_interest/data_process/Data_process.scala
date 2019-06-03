package content_interest.data_process

import java.text.SimpleDateFormat
import java.util.Calendar

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.Map

object Data_process {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181118"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Getting mzreader data")
    val categorys_map_file = args(1)
    get_mznews_data(sparkSession, categorys_map_file, job_date)

    my_log("Getting browser data and third_party data")
    get_browser_and_third_party_news_data(sparkSession, job_date)

    my_log("Getting third_party data")
    get_third_party_news_data(sparkSession, job_date)

    my_log("Getting notification data")
    get_notice_data(sparkSession, job_date)
  }

//  def get_user_reading_record(sparkSession: SparkSession, job_date: String) = {
//    val select_sql_user_reading = "select imei, article_id from from mzreader.dwd_app_stream_detail_reader where stat_date=" + job_date + " and event_name='view_article'"
//    print_sql(select_sql_user_reading)
//
//    val data_user_reading = sparkSession.sql(select_sql_user_reading).filter("imei is not null and article_id is not null").rdd.map(v => (v.get(0).toString, v.get(1).toString)).filter(v => v._1.length > 1 && v._2.length > 1)
//    val data_refined_user_reading = data_user_reading.map(v => (v, 1)).reduceByKey(_+_).map(v => (v._1._1, v._1._2, v._2))
//    printf("\n====>>>> raw record of user reading: %d\n", data_refined_user_reading.count())
//
//    val select_sql_article = "select fid, ftitle, fcontent, fcategory from mzreader.ods_t_article_c where stat_date>" + offset_date(job_date, month_offset = -1, day_offset = 0)
//    // val select_sql_article = "select fid, ftitle, fcontent, fcategory from mzreader.ods_t_article_c"
//    print_sql(select_sql_article)
//
//    val data_article = sparkSession.sql(select_sql_article).distinct().filter("fid is not null and ftitle is not null and fcontent is not null").rdd.map(v => (v.get(0).toString, (v.getString(1), v.getString(2), v.getString(3)))).filter(_._1.length > 1)
//    printf("\n====>>>> article: %d\n", data_article.count())
//    // printf("\n====>>>> article: %d\n", data_article.map(_._1).distinct().count())
//
//    val data_daily = data_refined_user_reading.map(v => (v._2, (v._1, v._3))).join(data_article).map(v => (v._2._1._1, v._1, v._2._2._1.trim, v._2._2._2.trim, v._2._2._3.trim, v._2._1._2))
//    printf("\n====>>>> refined record of user reading: %d\n", data_daily.count())
//
//    val data_daily_refined = data_daily.map(v => {
//      var uc = 0
//      var fcat_trans = "unk"
//      if (v._5 != null && v._5.length > 1){
//        fcat_trans = Labels.transform(v._5)
//        uc = 1
//      }
//      (v._1, v._2, v._3, v._4, v._5, fcat_trans, v._6, uc)
//    })
//    print_sql("uc vs non-us:")
//    numerical_label_distribute(data_daily_refined.map(v => (v._2, v._8)))
//
//    import sparkSession.implicits._
//    val save_table_name = "algo.up_yf_content_interest_user_reading_records_daily"
//    val cols = "imei string, fid string, ftitle string, fcontent string, fcategory string, fcategory_trans string, times int, uc int"
//    save_result_to_hive(sparkSession, data_daily_refined.toDF(), cols, save_table_name, job_date)
//  }

  def get_mznews_data(sparkSession: SparkSession, categorys_map_file: String, job_date: String) = {
    printf("\n====>>>> read categorys_map_file from : %s \n", categorys_map_file)
    val categorys_map: Map[String, String] = read_maps_from_file(sparkSession, categorys_map_file).collectAsMap()
    val used_categorys: Map[String, Int] = Array(
      "两性情感","两性情感,两性健康","两性情感,情感","两性情感,情感杂谈","两性情感,情感测试","两性情感,爱情攻略","两性情感,男人心理",

      "体育","体育,cba","体育,nba","体育,nfl","体育,乒乓球","体育,体操","体育,台球","体育,国内足球","体育,国际足球","体育,射击","体育,户外体育","体育,拳击","体育,排球","体育,搏击","体育,极限运动","体育,棋牌","体育,武术","体育,水上运动","体育,游泳跳水","体育,滑冰滑雪","体育,田径","体育,网球","体育,羽毛球","体育,赛车","体育,高尔夫",

      "健康","健康,两性知识","健康,健康养生","健康,健康新闻","健康,减肥健身","健康,心理健康","健康,疾病药品","健康,美容护肤",

      "军事","军事,中国军情","军事,军事历史","军事,国际军情","军事,女兵","军事,武器","军事,武警特警",

      "动漫","动漫,acgn情报","动漫,acgn杂谈","动漫,动漫cosplay","动漫,国漫","动漫,日韩动漫",

      "历史","历史,世界史","历史,中国古代史","历史,中国现代史","历史,中国近代史","历史,二战","历史,民国",

      "娱乐","娱乐,戏剧","娱乐,日韩明星","娱乐,明星","娱乐,欧美明星","娱乐,港台明星","娱乐,演出","娱乐,电影","娱乐,电视","娱乐,综艺","娱乐,美女写真","娱乐,音乐",

      "幽默",

      "生活服务",

      "房产","房产,家居","房产,房产市场","房产,政策","房产,租房","房产,风水",

      "摄影",

      "教育","教育,mba","教育,中小学","教育,中考","教育,公务员考试","教育,外语考试","教育,就业","教育,留学","教育,研究生考试","教育,职业考试","教育,高考",

      "文化","文化,乐器","文化,书法","文化,戏曲","文化,收藏","文化,民俗","文化,绘画",

      "旅游","旅游,出境游","旅游,境内游","旅游,攻略",

      "时尚","时尚,奢侈品","时尚,婚嫁","时尚,彩妆","时尚,时装","时尚,男士","时尚,美妆","时尚,配饰","时尚,鞋包",

      "星座","星座,心理测试","星座,星座爱情","星座,星座运势","星座,测试","星座,生肖运势","星座,算命",

      "汽车","汽车,二手车","汽车,新车","汽车,汽车优惠","汽车,汽车保养","汽车,汽车动态","汽车,汽车导购","汽车,汽车改装","汽车,汽车文化","汽车,汽车用品","汽车,汽车科技","汽车,汽车评测","汽车,用车","汽车,车展","汽车,驾驶",

      "游戏","游戏,PC游戏","游戏,单机游戏","游戏,手游","游戏,游戏cosplay","游戏,游戏主播","游戏,游戏赛事","游戏,网络游戏",

      "社会","社会,出行信息","社会,国际社会","社会,地方新闻","社会,奇闻","社会,民生","社会,法制社会","社会,灾难事故",

      "科学探索","科学探索,动物植物","科学探索,医学前沿","科学探索,天文","科学探索,生命科学","科学探索,科学前沿","科学探索,考古","科学探索,航天航空",

      "科技","科技,互联网","科技,产品设计","科技,创业","科技,家电","科技,手机","科技,数码","科技,智能硬件","科技,电商","科技,电脑","科技,科技前沿","科技,软件应用","科技,通信",

      "美食",

      "育儿","育儿,亲子","育儿,备孕","育儿,婴幼儿","育儿,孕期",

      "财经","财经,互联网金融","财经,保险","财经,债券","财经,地方经济","财经,基金","财经,外汇","财经,宏观经济","财经,期货","财经,理财","财经,经济民生","财经,股票","财经,贵金属","财经,银行"
    ).map(_.toLowerCase()).zipWithIndex.toMap

    printf("\n====>>>> categorys map: %d\n", categorys_map.size)
    printf("\n====>>>> used categorys: %d\n", used_categorys.size)

    // val select_sql = "select fid, ftitle, decode(unbase64(fcontent), 'utf-8') as fcontent, fcategory from mzreader.ods_t_article_c where stat_date>" + offset_date(job_date, month_offset = -6, day_offset = 0) + " and fresource_type==2"
    // got data from uc and baidu, only use pic and text
    // val select_sql = "select fid, ftitle, decode(unbase64(fcontent), 'utf-8') as fcontent, fcategory, fresource_type from mzreader.ods_t_article_c where stat_date=" + job_date + " and (fresource_type=2 or fresource_type=76) and (ftype=0 or ftype=1)"

    val select_sql = "select fid, ftitle, decode(unbase64(fcontent), 'utf-8') as fcontent, fcategory, fresource_type, ftype from mzreader.ods_t_article_c where stat_date=" + job_date
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("fid is not null and ftitle is not null and fcontent is not null").rdd.map(v => (v.get(0).toString.trim, v.getString(1).trim, v.getString(2).trim, v.getString(3), v(4).toString.toInt, v(5).toString.toInt)).filter(v => v._2.length > 1 && v._3.length > 1).map(v => {
      var category_name = "UNK"
      var category_id = -1
      var fresource_type = v._6
      var level = -1

      if(v._4 != null && v._4.length>1){
        val (cat, used) = transform(v._4, categorys_map, used_categorys)
        if(used){
          category_name = cat
          category_id = used_categorys(cat)
          if(category_name.trim.split(",").length == 2){
            level = 2
          }else{
            level = 1
          }
        }
//        fresource_type = v._6
      }

      (v._1, segment(refine_str(v._2)), segment(refine_str(stripHtml(v._3))), category_name, category_id, level, fresource_type, v._6)
    })
    printf("\n====>>>> data: %d\n", data.count())

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_content_interest_mznews_data"
    val cols = "fid string, ftitle string, fcontent string, fcategory string, fcategory_id int, level int, fresource_type int, ftype int"
    save_result_to_hive(sparkSession, data.toDF(), cols, save_table_name, job_date)

    // val unlabelled_data = data.filter(_._5 == -1).map(v => (v._1, v._2, v._3))
    val unlabelled_data = data // predict all mz_reader data

    printf("\n====>>>> unlabled data: %d\n", unlabelled_data.count())
    unlabelled_data.map(v => v._1 + "##yf##" + v._2).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/mzreader_title/" + job_date)
    unlabelled_data.map(v => v._1 + "##yf##" + v._3).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/mzreader_content/" + job_date)
  }

  def get_browser_and_third_party_news_data(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select fmd5, furl, ftitle, fcontent from uxip.dwd_browser_url_creeper where stat_date=" + job_date + " and src_type in ('com.ss.android.article.news', 'com.android.browser', 'com.ifeng.news2', 'com.netease.newsreader.activity', 'com.tencent.news')"
    print_sql(select_sql)

    val commerce_domains = Array("item.taobao.com", "s.click.taobao.com", "ai.m.taobao.com", "union.click.jd.com", "h5.m.taobao.com", "item.m.jd.com", "m.1688.com", "so.m.jd.com", "detail.m.tmall.com", "union-click.jd.com", "login.m.taobao.com", "m.taobao.com", "list.tmall.com", "m.vip.com", "s.m.taobao.com")
    val useless_domains = Array("a.app.qq.com", "mp.weixin.qq.com", "wappass.baidu.com", "mobile.baidu.com", "map.baidu.com", "y.10086.cn", "accounts.google.com", "dx.10086.cn", "m.cr173.com", "m.yiwan.com", "hanyu.baidu.com")
    val label_known_domains = Map("bbs.flyme.cn" -> "科技", "www.zybang.com" -> "教育", "detail.mall.meizu.com" -> "科技", "m.meizu.com" -> "科技", "m.120ask.com" -> "健康", "muzhi.baidu.com" -> "健康", "show.v.meizu.com" -> "科技", "baobao.baidu.com" -> "育儿", "m.ctrip.com" -> "旅游", "3g.club.xywy.com" -> "健康", "bbs.meizu.cn" -> "科技", "m.haodf.com" -> "健康", "www.flyme.cn" -> "科技", "h5.qichedaquan.com" -> "汽车", "m.babytree.com" -> "育儿", "i.flyme.cn" -> "科技", "m.flyme.cn" -> "科技", "mall.meizu.com" -> "科技", "a.9game.cn" -> "游戏", "car.h5.yiche.com" -> "汽车", "m.anjuke.com" -> "房产", "login.flyme.cn" -> "科技", "club.m.autohome.com.cn" -> "汽车")
    val useless_str = Array("百度一下", "下载百度网盘", "赞评论")

    val data = sparkSession.sql(select_sql).filter("fmd5 is not null and furl is not null and ftitle is not null and fcontent is not null").rdd.map(v => (v.getString(0), v.getString(1), refine_str(v.getString(2)), refine_str(v.getString(3)))).filter(v => v._3.length > 1 && v._4.length > 1).distinct()

    val data_refined = data.filter(v => !useless_str.contains(v._3)).map(v => {
      val tmp = v._2.split("""\/""")
      var domain = ""
      var label = "unk"
      if (tmp.length > 3) {
        domain = tmp(2)
        if (label_known_domains.contains(domain)){
          label = label_known_domains(domain)
        }
      }
      (v._1, domain, segment(v._3), segment(v._4), label)
    }).filter(v => !commerce_domains.contains(v._2) && !useless_domains.contains(v._2))
    printf("\n====>>>> data: %d\n", data_refined.count())

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_content_interest_browser_and_third_party_news_data"
    val cols = "fmd5 string, domain string, ftitle string, fcontent string, label string"
    save_result_to_hive(sparkSession, data_refined.toDF(), cols, save_table_name, job_date)

    val unlabelled_data = data_refined.filter(_._5 == "unk").map(v => (v._1, v._3, v._4))
    printf("\n====>>>> unlabled data: %d\n", unlabelled_data.count())
    unlabelled_data.map(v => v._1 + "##yf##" + v._2).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/browser_3p_title/" + job_date)
    unlabelled_data.map(v => v._1 + "##yf##" + v._3).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/browser_3p_content/" + job_date)
  }

  def get_third_party_news_data(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select fmd5, furl, ftitle, fcontent from uxip.dwd_browser_url_creeper where stat_date=" + job_date + " and src_type in ('com.ss.android.article.news', 'com.ifeng.news2', 'com.netease.newsreader.activity', 'com.tencent.news')"
    print_sql(select_sql)

    val commerce_domains = Array("item.taobao.com", "s.click.taobao.com", "ai.m.taobao.com", "union.click.jd.com", "h5.m.taobao.com", "item.m.jd.com", "m.1688.com", "so.m.jd.com", "detail.m.tmall.com", "union-click.jd.com", "login.m.taobao.com", "m.taobao.com", "list.tmall.com", "m.vip.com", "s.m.taobao.com")
    val useless_domains = Array("a.app.qq.com", "mp.weixin.qq.com", "wappass.baidu.com", "mobile.baidu.com", "map.baidu.com", "y.10086.cn", "accounts.google.com", "dx.10086.cn", "m.cr173.com", "m.yiwan.com", "hanyu.baidu.com")
    val label_known_domains = Map("bbs.flyme.cn" -> "科技", "www.zybang.com" -> "教育", "detail.mall.meizu.com" -> "科技", "m.meizu.com" -> "科技", "m.120ask.com" -> "健康", "muzhi.baidu.com" -> "健康", "show.v.meizu.com" -> "科技", "baobao.baidu.com" -> "育儿", "m.ctrip.com" -> "旅游", "3g.club.xywy.com" -> "健康", "bbs.meizu.cn" -> "科技", "m.haodf.com" -> "健康", "www.flyme.cn" -> "科技", "h5.qichedaquan.com" -> "汽车", "m.babytree.com" -> "育儿", "i.flyme.cn" -> "科技", "m.flyme.cn" -> "科技", "mall.meizu.com" -> "科技", "a.9game.cn" -> "游戏", "car.h5.yiche.com" -> "汽车", "m.anjuke.com" -> "房产", "login.flyme.cn" -> "科技", "club.m.autohome.com.cn" -> "汽车")
    val useless_str = Array("百度一下", "下载百度网盘", "赞评论")

    val data = sparkSession.sql(select_sql).filter("fmd5 is not null and furl is not null and ftitle is not null and fcontent is not null").rdd.map(v => (v.getString(0), v.getString(1), refine_str(v.getString(2)), refine_str(v.getString(3)))).filter(v => v._3.length > 1 && v._4.length > 1).distinct()

    val data_refined = data.filter(v => !useless_str.contains(v._3)).map(v => {
      val tmp = v._2.split("""\/""")
      var domain = ""
      var label = "unk"
      if (tmp.length > 3) {
        domain = tmp(2)
        if (label_known_domains.contains(domain)){
          label = label_known_domains(domain)
        }
      }
      (v._1, domain, segment(v._3), segment(v._4), label)
    }).filter(v => !commerce_domains.contains(v._2) && !useless_domains.contains(v._2))
    printf("\n====>>>> data: %d\n", data_refined.count())

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_content_interest_third_party_news_data"
    val cols = "fmd5 string, domain string, ftitle string, fcontent string, label string"
    save_result_to_hive(sparkSession, data_refined.toDF(), cols, save_table_name, job_date)

    val unlabelled_data = data_refined.filter(_._5 == "unk").map(v => (v._1, v._3, v._4))
    printf("\n====>>>> unlabled data: %d\n", unlabelled_data.count())
    unlabelled_data.map(v => v._1 + "##yf##" + v._2).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/3p_title/" + job_date)
    unlabelled_data.map(v => v._1 + "##yf##" + v._3).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/3p_content/" + job_date)
  }

  def get_notice_data(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select imei, misc_map ['title'] title, misc_map ['content'] content from uxip.dwd_app_action_detail where stat_date = " + job_date + " and pkg_name='com.android.systemui' and event_name in ('notifiction_receive', 'notifiction_icon_intent_click') and misc_map ['category'] in ('news','recommend','subscribe') and misc_map['PackageName'] not in ('com.taobao.taobao', 'com.xunmeng.pinduoduo', 'com.jingdong.app.mall', 'com.achievo.vipshop', 'com.xingin.xhs', 'com.tmall.wireless')"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and title is not null and content is not null").rdd.map(v => (v.getString(0), v.getString(1), v.getString(2))).filter(v => v._2.length > 1 && v._3.length > 1).map(v => ((v._2, v._3), Array(v._1)))
    val data_refined = data.reduceByKey(_++_).map(v => (segment(refine_str(v._1._1)), segment(refine_str(v._1._2)), v._2.mkString(" ").trim))

    printf("\n====>>>> notice data: %d\n", data_refined.count())

//    val notice_ods = data.map(v => (v._2.hashCode, v._2, v._3)).distinct().zipWithIndex()
//    printf("\n====>>>> notice ods title: %d\n", notice_ods.count())
//
//    val notice_records = data.map(v => ((v._1, notice_ods.lookup((v._2, v._3)).head), 1)).reduceByKey(_+_).map(v => (v._1, v._1._2.toString, v._2))
//    printf("\n====>>>> notice user: %d\n", notice_records.map(_._1).distinct().count())
//    printf("\n====>>>> notice user actions: %d\n", notice_records.count())
    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_content_interest_notice_records"
    val cols = "ftitle string, fcontent string, imeis string"
    save_result_to_hive(sparkSession, data_refined.toDF(), cols, save_table_name, job_date)

    data_refined.map(v => v._3 + "##yf##" + v._1).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/notice_title/" + job_date)
    data_refined.map(v => v._3 + "##yf##" + v._2).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/notice_content/" + job_date)
  }

  def get_notice_data_new(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select trim(misc_map['key']) as fid, misc_map ['title'] title, misc_map ['content'] content from uxip.dwd_app_action_detail where stat_date = " + job_date + " and pkg_name='com.android.systemui' and event_name in ('notifiction_receive', 'notifiction_icon_intent_click') and misc_map ['category'] in ('news','recommend','subscribe') and misc_map['PackageName'] not in ('com.taobao.taobao', 'com.xunmeng.pinduoduo', 'com.jingdong.app.mall', 'com.achievo.vipshop', 'com.xingin.xhs', 'com.tmall.wireless')"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("fid is not null and title is not null and content is not null").rdd.map(v => (v.getString(0), v.getString(1), v.getString(2))).filter(v => v._2.length > 1 && v._3.length > 1).map(v => (v._1, (v._2, v._3))).reduceByKey((a, b) => a).map(v => (v._1, segment(refine_str(v._2._1)), segment(refine_str(v._2._2))))

    printf("\n====>>>> notice data: %d\n", data.count())

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_content_interest_notice_records_new"
    val cols = "fid string, ftitle string, fcontent string"
    save_result_to_hive(sparkSession, data.toDF(), cols, save_table_name, job_date)

    data.map(v => v._1 + "##yf##" + v._2).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/notice_title/" + job_date)
    data.map(v => v._1 + "##yf##" + v._3).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/unlabeled_data/notice_content/" + job_date)
  }

  def init_job(job_time: String): (SparkSession, String) = {
    val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val year: Int = job_time.substring(0, 4).trim.toInt
    val month: Int = job_time.substring(4, 6).trim.toInt
    val day: Int = job_time.substring(6, 8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year, month - 1, day)
    val job_date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)
    (sparkSession, job_date)
  }

  def my_log(g: String) = {
    printf("\n\n++++++++\n++++++++ %s\n++++++++\n\n", g)
  }

  def print_sql(ss: String) = {
    printf("\n====>>>> %s\n", ss)
  }

  def offset_date(date: String, month_offset: Int, day_offset: Int) = {
    val year: Int = date.substring(0, 4).trim.toInt
    val month: Int = date.substring(4, 6).trim.toInt
    val day: Int = date.substring(6, 8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year, month - 1 + month_offset, day + day_offset)
    val d = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)
    d
  }

  def save_result_to_hive(sparkSession: SparkSession, result: DataFrame, columns: String, table_name: String, job_date: String) = {
    result.createOrReplaceTempView("temp")

    if (job_date != "") {
      printf("\n====>>>> save result to: %s with partition %s\n", table_name, job_date)
      val create_sql = "create table if not exists " + table_name + "(" + columns + ") partitioned by (stat_date bigint) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS RCFILE"
      val insert_sql = "insert overwrite table " + table_name + " partition(stat_date = " + job_date + ") select * from temp"
      printf("\n====>>>> %s\n====>>>> %s\n", create_sql, insert_sql)
      sparkSession.sql(create_sql)
      sparkSession.sql(insert_sql)
    }
    else {
      printf("\n====>>>> save result to: %s with no partition\n", table_name)
      val create_sql = "create table if not exists " + table_name + "(" + columns + ") ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS RCFILE"
      val insert_sql = "insert overwrite table " + table_name + " select * from temp"
      printf("\n====>>>> %s\n====>>>> %s\n", create_sql, insert_sql)
      sparkSession.sql(create_sql)
      sparkSession.sql(insert_sql)
    }
  }

  def numerical_label_distribute(labeled_data: RDD[(String, Double)]) = {
    val labeled_data_count = labeled_data.count()
    labeled_data.map(v => (v._2, 1)).reduceByKey(_+_).collect().sortBy(_._2).foreach(f = v => {
      printf("\n====>>>> label_%s: %d, all: %d, ratio: %.4f\n", v._1, v._2, labeled_data_count, v._2 * 1.0 / labeled_data_count)
    })
  }

  def transform(ss: String, categorys_map: collection.Map[String, String], used_categorys: Map[String, Int]) = {
    var cat = ss.trim.toLowerCase.replace("其它", "其他").replaceAll("[ _-]+", ",")
    cat = categorys_map.getOrElse(cat, cat)
    val used = used_categorys.contains(cat)
    (cat, used)
    //cat = allMap.getOrElse(cat.split(",")(0), cat)
  }

  def stripHtml(ss: String): String = {
    if (ss == null || ss.isEmpty) ""
    else {
      ss.replaceAll("\n"," ").replaceAll("<script>.*?</script>","")
        .replaceAll("(</p>|</br>)\n*", "\n")
        .replaceAll("<[^>]+?>" , " ")
        .replaceAll("(点击加载图片)|(查看原文)|(图片来源)|([\\-#=]{3,})", " ")
        .replaceAll("\\s*\n\\s*", "\n")
        .replaceAll("[ \t]+", " ")
    }
  }

  def segment(s: String) = {
    val sp_filter = new StopRecognition()
    sp_filter.insertStopNatures("w")

    var split_s = ""
    if (s != null && !s.trim.isEmpty) {
      try {
        split_s = ToAnalysis.parse(s).recognition(sp_filter).toStringWithOutNature(" ")
      } catch {
        case e: Exception => printf("\n====>>>> %s\n====>>>> %s\n", s, e)
      }
    }
    split_s
  }

  def refine_str(s: String) = {
    if(s.length < 1)
      ""
    else s.replaceAll("[^\u4e00-\u9FCB]+", " ").replace("\n", " ")
  }

  def read_maps_from_file(sparkSession: SparkSession, file: String) = {
    val raw_data = sparkSession.sparkContext.textFile(file)
    raw_data.map(v => {
      val tmp = v.split("##")
      (tmp(0).toLowerCase(), tmp(1).toLowerCase())
    }).reduceByKey((a, b) => b)
  }
}

