package shopping_interrest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.recognition.impl.StopRecognition
import org.apache.spark.rdd.RDD


object Data_Prepare {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181010"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Prepare notification data")
    val notification_table = "uxip.dwd_app_action_detail"
    prepare_notification_date(sparkSession, notification_table, job_date)

    my_log("Getting imei info")
    val fmd5_imei = get_imei_fdm5(sparkSession, job_date)
    val fmd5_imei_browser = get_imei_fdm5_browser(sparkSession, job_date)

    my_log("Prepare the third-party e-commerce data")
    prepare_third_party_ecommerce_data(sparkSession, fmd5_imei, job_date)

    my_log("Prepare the browser data")
    prepare_browser_data(sparkSession, fmd5_imei_browser, job_date)
  }

  def prepare_notification_date(sparkSession: SparkSession, table_name: String, job_date: String) = {
    //  and misc_map['PackageName'] in ('com.taobao.taobao', 'com.xunmeng.pinduoduo', 'com.jingdong.app.mall', 'com.achievo.vipshop', 'com.xingin.xhs', 'com.tmall.wireless')
    val select_sql = "SELECT imei, misc_map['title'] as title, misc_map['content'] as content, misc_map['PackageName'] as source, event_name from " + table_name + " where stat_date=" + job_date + " and event_name in ('notifiction_receive', 'notifiction_icon_intent_click') and pkg_name = 'com.android.systemui' and country='CN' and lla='zh_CN' and is_oversea=0 and misc_map['PackageName'] in ('com.taobao.taobao', 'com.xunmeng.pinduoduo', 'com.jingdong.app.mall', 'com.achievo.vipshop', 'com.xingin.xhs', 'com.tmall.wireless')"
    print_sql(select_sql)

    val notification_data_raw = sparkSession.sql(select_sql).filter("imei is not null and title is not null and content is not null and source is not null and event_name is not null").distinct().rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v(3).toString, v(4).toString))
    val notification_data_raw_count = notification_data_raw.count()
    printf("\n====>>>> notification raw data: %d\n", notification_data_raw_count)

    import sparkSession.implicits._
    val notification_data_seg = notification_data_raw.map(v => (v._1, segment(v._2), segment(v._3), v._4, v._5)).filter(v => v._2 != "null" && v._3 != "null").toDF("imei", "title", "content", "source", "event_name")
    val save_table_name = "algo.up_yf_shopping_interest_notification"
    val cols = "imei string, title string, content string, source string, event_name string"
    if (notification_data_raw_count > 20000) {
      save_result_to_hive(sparkSession, notification_data_seg, cols, save_table_name, job_date)
    } else {
      printf("\n====>>>> no saving of notification data")
    }
  }

  def prepare_third_party_ecommerce_data(sparkSession: SparkSession, fdm5_imei: RDD[(String, String)], job_date: String) = {
    val select_sql = "SELECT fmd5, furl, ftitle, fkeywords, fcontent, fdescription, src_type from uxip.dwd_browser_url_creeper where stat_date=" + job_date+ " and src_type in ('com.taobao.taobao', 'com.xunmeng.pinduoduo', 'com.jingdong.app.mall', 'com.achievo.vipshop', 'com.xingin.xhs', 'com.tmall.wireless')"
    print_sql(select_sql)
    val data = sparkSession.sql(select_sql).filter("fmd5 is not null and furl is not null and ftitle is not null and fcontent is not null and fdescription is not null and src_type is not null").distinct().rdd.map(v => (v(0).toString, (v(1).toString, v(2).toString, v.getString(3), v(4).toString, v(5).toString, v(6).toString)))
    val data_count = data.count()
    printf("\n====>>>> the raw data of third-party commerce: %d\n", data_count)

    val data_refined = data.join(fdm5_imei).map(v => (v._2._2, v._2._1._1, v._2._1._2, v._2._1._3, v._2._1._4, v._2._1._5, v._2._1._6))
    printf("\n====>>>> map to imei: %d\n", data_refined.count())

    import sparkSession.implicits._
    val data_seg = data_refined.map(v => (v._1, v._2, segment(v._3), v._4, v._5, segment(v._6), v._7)).filter(v => v._3 != "null" && v._6 != "null").toDF()

    val save_table_name = "algo.up_yf_shopping_interest_3d_ecommerce"
    val cols = "fmd5 string, url string, title string, keywords string, content string, descripthon string, source string"
    if (data_count > 20000) {
      save_result_to_hive(sparkSession, data_seg, cols, save_table_name, job_date)
    } else {
      printf("\n====>>>> no saving of third party e-commerce data")
    }
  }

  def prepare_browser_data(sparkSession: SparkSession, fmd5_imei: RDD[(String, String)], job_date: String) = {
    val select_sql = "SELECT fmd5, furl, ftitle, fcontent, fdescription from uxip.dwd_browser_url_creeper where stat_date=" + job_date + " and src_type='com.android.browser'"
    print_sql(select_sql)

    val domains = Array("item.taobao.com", "s.click.taobao.com", "ai.m.taobao.com", "union.click.jd.com", "h5.m.taobao.com", "item.m.jd.com", "m.1688.com", "so.m.jd.com", "detail.m.tmall.com", "union-click.jd.com", "login.m.taobao.com", "m.taobao.com", "list.tmall.com", "m.vip.com", "s.m.taobao.com")

    val data = sparkSession.sql(select_sql).filter("fmd5 is not null and furl is not null and ftitle is not null and fcontent is not null and fdescription is not null").rdd.map(v =>(v(0).toString, v(1).toString, v(2).toString, v(3).toString, v(4).toString)).distinct().filter(_._2.length > 0).map(v => {
      val tmp = v._2.split("""\/""")
      var domain = ""
      if (tmp.length > 3) {
        domain = tmp(2)
      }
      (v._1, domain, segment(v._3), segment(v._4), segment(v._5))
    }).filter(v => domains.contains(v._2))
    printf("\n====>>>> data of browser: %d\n", data.count())

    val data_refined = data.map(v => (v._1, (v._2, v._3, v._4, v._5))).join(fmd5_imei).map(v => (v._2._2, v._2._1._1, v._2._1._2, v._2._1._3, v._2._1._4))
    val data_refined_count = data_refined.count()
    printf("\n====>>>> map to imei: %d\n", data_refined_count)

    data_refined.map(v => (v._2, 1)).reduceByKey(_+_).collect().sortBy(_._2).foreach(v => {
      printf("\n====>>>> label_%s: %d, all: %d, ratio: %.4f\n", v._1, v._2, data_refined_count, v._2 * 1.0 / data_refined_count)
    })

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_shopping_interest_browser"
    val cols = "fid string, domain string, ftitle string, fcontent string, fdescription string"
    if (data_refined_count > 5000) {
      save_result_to_hive(sparkSession, data_refined.toDF(), cols, save_table_name, job_date)
    }
  }

  def get_imei_fdm5(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select url_md5, imei from uxip.ods_userprofile_url where stat_date=" + offset_date(job_date, month_offset = 0, day_offset = -1)
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("url_md5 is not null and imei is not null").rdd.map(v => (v(0).toString, v(1).toString)).filter(v => v._1.length > 1 && v._2.length > 1).distinct()
    printf("\n====>>>> fdm5-imei: %d\n", data.count())
    data
  }

  def get_imei_fdm5_browser(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select url_md5, imei from uxip.dwm_browser_url_detail_mid where stat_date=" + offset_date(job_date, month_offset = 0, day_offset = -1)
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("url_md5 is not null and imei is not null").rdd.map(v => (v(0).toString, v(1).toString)).filter(v => v._1.length > 1 && v._2.length > 1).distinct()
    printf("\n====>>>> fdm5-imei-browser: %d\n", data.count())
    data
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

  def init_job(job_time: String): (SparkSession, String) = {
    val sparkSession =  SparkSession.builder().enableHiveSupport().getOrCreate()

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

  def offset_date(date: String, month_offset: Int, day_offset: Int) = {
    val year: Int = date.substring(0, 4).trim.toInt
    val month: Int = date.substring(4, 6).trim.toInt
    val day: Int = date.substring(6, 8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year, month - 1 + month_offset, day + day_offset)
    val d = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)
    d
  }

  def my_log(g: String) = {
    printf("\n\n++++++++\n++++++++ %s\n++++++++\n\n", g)
  }

  def print_sql(ss: String) = {
    printf("\n====>>>> %s\n", ss)
  }

  def save_result_to_hive(sparkSession: SparkSession, result: DataFrame, columns: String, table_name: String, job_date: String) = {
    result.createOrReplaceTempView("temp")

    if (job_date != ""){
      printf("\n====>>>> save result to: %s with partition %s\n", table_name, job_date)
      val create_sql = "create table if not exists " + table_name + "(" + columns + ") partitioned by (stat_date bigint) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS RCFILE"
      val insert_sql = "insert overwrite table " + table_name + " partition(stat_date = " + job_date + ") select * from temp"
      printf("\n====>>>> %s\n====>>>> %s\n", create_sql, insert_sql)
      sparkSession.sql(create_sql)
      sparkSession.sql(insert_sql)
    }
    else{
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
    labeled_data.map(v => (v._2, 1)).reduceByKey(_+_).collect().sortBy(_._1).foreach(f = v => {
      printf("\n====>>>> label_%s: %d, all: %d, ratio: %.4f\n", v._1, v._2, labeled_data_count, v._2 * 1.0 / labeled_data_count)
    })
  }

}
