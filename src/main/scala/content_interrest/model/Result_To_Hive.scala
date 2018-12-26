package content_interrest.model

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Result_To_Hive {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181118"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Getting inferring result")
    get_inferring_result_mzreader(sparkSession, refined_flag = true, job_date)
    get_inferring_result_browser_3p(sparkSession, refined_flag = true, job_date)
    get_inferring_result_notice(sparkSession, refined_flag = true, job_date)
    get_inferring_result_3p(sparkSession, refined_flag = false, job_date)
  }

  def get_inferring_result_mzreader(sparkSession: SparkSession, refined_flag: Boolean, job_date: String) = {
    val mzread_result_path = "/apps/recommend/models/wind/content_interrest/unlabeled_data/mzreader_title/" + job_date + "/mzreader.title.result"
    print_sql(mzread_result_path)
    val mzread_result = get_result(sparkSession, mzread_result_path, refined_flag)

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_content_interest_mzreader_result"
    val cols = "fid string, label_1 string, pred_1 double, label_2 string, pred_2 double"
    save_result_to_hive(sparkSession, mzread_result.toDF(), cols, save_table_name, job_date)
  }

  def get_inferring_result_browser_3p(sparkSession: SparkSession, refined_flag: Boolean, job_date: String) = {
    val browser_3p_result_path = "/apps/recommend/models/wind/content_interrest/unlabeled_data/browser_3p_title/" + job_date + "/browser.3p.title.result"
    print_sql(browser_3p_result_path)
    val browser_3p_result = get_result(sparkSession, browser_3p_result_path, refined_flag)

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_content_interest_browser_3p_result"
    val cols = "fmd5 string, label_1 string, pred_1 double, label_2 string, pred_2 double"
    save_result_to_hive(sparkSession, browser_3p_result.toDF(), cols, save_table_name, job_date)
  }

  def get_inferring_result_3p(sparkSession: SparkSession, refined_flag: Boolean, job_date: String) = {
    val third_party_result_path = "/apps/recommend/models/wind/content_interrest/unlabeled_data/3p_title/" + job_date + "/3p.title.result"
    print_sql(third_party_result_path)
    val third_party_result = get_result(sparkSession, third_party_result_path, refined_flag)

    val bad_case = third_party_result.filter(_._3 < 0.5).map(v => (v._2, List((v._1, v._3)))).reduceByKey(_++_).map(v => scala.util.Random.shuffle(v._2).take(500).map(s => (s._1, v._1, s._2))).flatMap(v => v)
    printf("\n====>>>> bad case in third_party: %d\n", bad_case.count())

    """
      |SELECT a.fmd5, b.ftitle, b.fcontent, a.label_1, a.pred_1 from algo.up_yf_content_interest_3p_result a join algo.up_yf_content_interest_third_party_news_data b on a.fmd5=b.fmd5 where a.stat_date=20181224 and b.stat_date=20181224
    """.stripMargin

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_content_interest_3p_result"
    val cols = "fmd5 string, label_1 string, pred_1 double"
    save_result_to_hive(sparkSession, bad_case.toDF(), cols, save_table_name, job_date)
  }

  def get_inferring_result_notice(sparkSession: SparkSession, refined_flag: Boolean, job_date: String) = {
    val notice_result_path = "/apps/recommend/models/wind/content_interrest/unlabeled_data/notice_content/" + job_date + "/notice.content.result"
    print_sql(notice_result_path)
    var notice_result = get_result(sparkSession, notice_result_path, refined_flag)

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_content_interest_notice_result"
    val cols = "imeis string, label_1 string, pred_1 double, label_2 string, pred_2 double"
    save_result_to_hive(sparkSession, notice_result.toDF(), cols, save_table_name, job_date)
  }

  def get_result(sparkSession: SparkSession, result_file: String, refine_flag: Boolean) = {
    val data = sparkSession.sparkContext.textFile(result_file).map(_.split("\\|")).filter(_.length == 3).map(v => (v(0), v(1), v(2))).map(v => {
      var label_first_level = ""
      var pred_first_level = -1.0
      var label_second_level = ""
      var pred_second_level = -1.0

      if (v._2.length > 1){
        val tmp = v._2.split(",")
        if (tmp.length == 2){
          pred_first_level = tmp(1).toDouble
          label_first_level = tmp(0).stripPrefix("__label__")
        }
      }

      if (v._3.length > 1){
        val tmp = v._3.split(",")
        if (tmp.length == 2){
          pred_second_level = tmp(1).toDouble
          label_second_level = tmp(0).stripPrefix("__label__")
        }
      }

      (v._1, label_first_level, pred_first_level, label_second_level, pred_second_level)
    }).filter(v => v._1.length > 1 && v._2.length > 1 && v._3 != -1.0 && v._4.length > 1 && v._5 != -1.0).distinct()

    printf("\n====>>>> result: %d\n", data.count())

    if (refine_flag) {
      refine_result(data)
    } else {
      data
    }
  }

  def refine_result(data: RDD[(String, String, Double, String, Double)]) = {
    val used_categorys = Array(
      "两性情感","两性情感,两性健康","两性情感,情感","两性情感,情感杂谈","两性情感,情感测试","两性情感,爱情攻略","两性情感,男人心理",

      "体育","体育,cba","体育,nba","体育,nfl","体育,乒乓球","体育,体操","体育,台球","体育,国内足球","体育,国际足球","体育,射击","体育,户外体育","体育,拳击","体育,排球","体育,搏击","体育,极限运动","体育,棋牌","体育,武术","体育,水上运动","体育,游泳跳水","体育,滑冰滑雪","体育,田径","体育,网球","体育,羽毛球","体育,赛车","体育,高尔夫",

      "健康","健康,两性知识","健康,健康养生","健康,健康新闻","健康,减肥健身","健康,心理健康","健康,疾病药品","健康,美容护肤",

      "军事","军事,中国军情","军事,军事历史","军事,国际军情","军事,女兵","军事,武器","军事,武警特警",

      "动漫","动漫,acgn情报","动漫,acgn杂谈","动漫,动漫cosplay","动漫,国漫","动漫,日韩动漫",

      "历史","历史,世界史","历史,中国古代史","历史,中国现代史","历史,中国近代史","历史,二战","历史,民国",

      "娱乐","娱乐,戏剧","娱乐,日韩明星","娱乐,明星","娱乐,欧美明星","娱乐,港台明星","娱乐,演出","娱乐,电影","娱乐,电视","娱乐,综艺","娱乐,美女写真","娱乐,音乐",

      "宠物",

      "干货",

      "幽默",

      "彩票",

      "房产","房产,家居","房产,房产市场","房产,政策","房产,租房","房产,风水",

      "摄影",

      "教育","教育,mba","教育,中小学","教育,中考","教育,公务员考试","教育,外语考试","教育,就业","教育,留学","教育,研究生考试","教育,职业考试","教育,高考",

      "文化","文化,乐器","文化,书法","文化,戏曲","文化,收藏","文化,民俗","文化,绘画",

      "旅游","旅游,出境游","旅游,境内游","旅游,攻略",

      "时尚","时尚,奢侈品","时尚,婚嫁","时尚,彩妆","时尚,时装","时尚,男士","时尚,美妆","时尚,配饰","时尚,鞋包",

      "时政","时政,国内","时政,国际",

      "星座","星座,心理测试","星座,星座爱情","星座,星座运势","星座,测试","星座,生肖运势","星座,算命",

      "汽车","汽车,二手车","汽车,新车","汽车,汽车优惠","汽车,汽车保养","汽车,汽车动态","汽车,汽车导购","汽车,汽车改装","汽车,汽车文化","汽车,汽车用品","汽车,汽车科技","汽车,汽车评测","汽车,用车","汽车,车展","汽车,驾驶",

      "游戏","游戏,PC游戏","游戏,单机游戏","游戏,手游","游戏,游戏cosplay","游戏,游戏主播","游戏,游戏赛事","游戏,网络游戏",

      "社会","社会,出行信息","社会,国际社会","社会,地方新闻","社会,奇闻","社会,民生","社会,法制社会","社会,灾难事故","社会,生活服务",

      "科学探索","科学探索,动物植物","科学探索,医学前沿","科学探索,天文","科学探索,生命科学","科学探索,科学前沿","科学探索,考古","科学探索,航天航空",

      "科技","科技,互联网","科技,产品设计","科技,创业","科技,家电","科技,手机","科技,数码","科技,智能硬件","科技,电商","科技,电脑","科技,科技前沿","科技,软件应用","科技,通信",

      "美文",

      "美食",

      "育儿","育儿,亲子","育儿,备孕","育儿,婴幼儿","育儿,孕期",

      "财经","财经,互联网金融","财经,保险","财经,债券","财经,地方经济","财经,基金","财经,外汇","财经,宏观经济","财经,期货","财经,理财","财经,经济民生","财经,股票","财经,贵金属","财经,银行"
    ).map(_.toLowerCase()).zipWithIndex.toMap

    printf("\n====>>>> data(before refine): %d\n", data.count())
    val refined_data = data.map(v => {
      var label_1 = v._2
      var pred_1 = v._3
      var label_2 = v._4
      var pred_2 = v._5
      var invalid = 0

      val lable_1_tmp = label_1.split("_")(0)
      val tmp = label_2.trim.split("-")
      if (tmp.length == 2) {
        if (lable_1_tmp != tmp(0)) {
          if (pred_1 <= 0.5 && pred_2 <= 0.5) {
            invalid = 1
          } else {
            if (pred_2 > pred_1 && pred_2 >= 0.7) {
              label_1 = tmp(0) + "_" + used_categorys(tmp(0))
              pred_1 = 1.5
            } else {
              label_2 = label_1 + "-其它"
              pred_2 = 1.5
            }
          }
        }
      }
      (v._1, label_1, pred_1, label_2, pred_2, invalid)
    }).filter(_._6 == 0).map(v => (v._1, v._2, v._3, v._4, v._5))
    printf("\n====>>>> data(before refine): %d\n", refined_data.count())
    refined_data
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
}
