package shopping_interrest.fasttext

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Result_To_DB {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20180810"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Getting labels")
    val labels = get_labels(sparkSession)

    my_log("Third party commerce")
    get_result_third_p_commerce_all(sparkSession, labels, job_date)

    my_log("Notification")
    get_result_notification_all(sparkSession, labels, job_date)

    my_log("Browser")
    get_result_browser_all(sparkSession, labels, job_date)

  }

  def get_labels(sparkSession: SparkSession) = {
    val select_sql = "select category_1, label_1, category_2, label_2, category_3, label_3 from algo.up_yf_shopping_interest_jd_labelled_data_1"
    print_sql(select_sql)
    val labels = sparkSession.sql(select_sql).rdd.map(v => (v(0).toString, v(1).toString.split("-")(1), v(2).toString, v(3).toString.split("-")(1), v(4).toString, v(5).toString.split("-")(1)))
    labels
  }

  def get_result(sparkSession: SparkSession, result_file: String, labels_info: RDD[(String, String)], job_date: String) = {
    val labels_info_map = labels_info.collectAsMap()
    val data = sparkSession.sparkContext.textFile(result_file).map(_.split("\\|")).filter(_.length == 3).map(v => (v(0), v(1), v(2))).map(v => {
      var label = ""
      if (v._3.length > 1){
        val tmp = v._3.split("__")
        if (tmp.length == 3){
          label = labels_info_map(tmp(2))
        }
      }
      (v._1, v._2, v._3, label)
    }).filter(v => v._1.length>1 && v._2.length>1 && v._3.length>1).distinct()

    printf("\n====>>>> result: %d\n", data.count())
    data
  }

//  def get_result_third_p_commerce(sparkSession: SparkSession, result_thirdp: String, labels_info: RDD[(String, String)], job_date: String) = {
//    import sparkSession.implicits._
//    val labels_info_map = labels_info.collectAsMap()
//    val data = sparkSession.sparkContext.textFile(result_thirdp).map(_.split("\\|")).filter(_.length == 3).map(v => (v(0), v(1), v(2))).map(v => {
//      var label = ""
//      if (v._3.length > 1){
//        val tmp = v._3.split("__")
//        if (tmp.length == 3){
//          label = labels_info_map(tmp(2))
//        }
//      }
//      (v._1, v._2, v._3, label)
//    }).filter(v => v._1.length>1 && v._2.length>1 && v._3.length>1).distinct()
//
//    printf("\n====>>>> third_p_commerce result: %d\n", data.count())
//    data
//  }

  def get_result_third_p_commerce_all(sparkSession: SparkSession, labels: RDD[(String, String, String, String, String, String)], job_date: String) = {
    val result_thirdp_first_level_file = "/apps/recommend/models/wind/shopping_interrest/result/third.party.commerce.first.level.result"
    val result_thirdp_second_level_file = "/apps/recommend/models/wind/shopping_interrest/result/third.party.commerce.second.level.result"
    val result_thirdp_third_level_file = "/apps/recommend/models/wind/shopping_interrest/result/third.party.commerce.third.level.result"
    print_sql(result_thirdp_first_level_file)
    print_sql(result_thirdp_second_level_file)
    print_sql(result_thirdp_third_level_file)

    val result_thirdp_first_level = get_result(sparkSession, result_thirdp_first_level_file, labels.map(v => (v._2, v._1)).distinct(), job_date).map(v => ((v._1, v._2), (v._3, v._4)))
    val result_thirdp_second_level = get_result(sparkSession, result_thirdp_second_level_file, labels.map(v => (v._4, v._3)).distinct(), job_date).map(v => ((v._1, v._2), (v._3, v._4)))
    val result_thirdp_third_level = get_result(sparkSession, result_thirdp_third_level_file, labels.map(v => (v._6, v._5)).distinct(), job_date).map(v => ((v._1, v._2), (v._3, v._4)))

    import sparkSession.implicits._
    val result_thirdp_finnal = result_thirdp_first_level.join(result_thirdp_second_level).map(v => ((v._1._1, v._1._2), (v._2._1._1, v._2._1._2, v._2._2._1, v._2._2._2))).join(result_thirdp_third_level).map(v => (v._1._1, v._1._2, v._2._1._1, v._2._1._2, v._2._1._3, v._2._1._4, v._2._2._1, v._2._2._2)).toDF("fmd5", "title", "pred_1", "label_1", "pred_2", "label_2", "pred_3", "label_3")
    result_thirdp_finnal.show(5)
    val save_table_name_thirdp = "algo.up_yf_shopping_interest_3d_ecommerce_result_all"
    val cols_thirdp = "fmd5 string, title string, pred_1 string, label_1 string, pred_2 string, label_2 string, pred_3 string, label_3 string"
    save_result_to_hive(sparkSession, result_thirdp_finnal, cols_thirdp, save_table_name_thirdp, job_date)
  }

//  def get_result_notification(sparkSession: SparkSession, result_notification: String, labels_info: RDD[(String, String)], job_date: String) = {
//    import sparkSession.implicits._
//    val labels_info_map = labels_info.collectAsMap()
//    val data = sparkSession.sparkContext.textFile(result_notification).map(_.split("\\|")).filter(_.length == 3).map(v => (v(0), v(1), v(2))).map(v => {
//      var label = ""
//      if (v._3.length > 1){
//        val tmp = v._3.split("__")
//        if (tmp.length == 3){
//          label = labels_info_map(tmp(2))
//        }
//      }
//      (v._1, v._2, v._3, label)
//    }).filter(v => v._1.length>1 && v._2.length>1 && v._3.length>1).distinct()
//
//    printf("\n====>>>> notification result: %d\n", data.count())
//    data
//  }

  def get_result_notification_all(sparkSession: SparkSession, labels: RDD[(String, String, String, String, String, String)], job_date: String) = {
    val result_notification_first_level_file = "/apps/recommend/models/wind/shopping_interrest/result/notification.first.level.result"
    val result_notification_second_level_file = "/apps/recommend/models/wind/shopping_interrest/result/notification.second.level.result"
    val result_notification_third_level_file = "/apps/recommend/models/wind/shopping_interrest/result/notification.third.level.result"
    print_sql(result_notification_first_level_file)
    print_sql(result_notification_second_level_file)
    print_sql(result_notification_third_level_file)

    val result_notification_first_level = get_result(sparkSession, result_notification_first_level_file, labels.map(v => (v._2, v._1)).distinct(), job_date).map(v => ((v._1, v._2), (v._3, v._4)))
    val result_notification_second_level = get_result(sparkSession, result_notification_second_level_file, labels.map(v => (v._4, v._3)).distinct(), job_date).map(v => ((v._1, v._2), (v._3, v._4)))
    val result_notification_third_level = get_result(sparkSession, result_notification_third_level_file, labels.map(v => (v._6, v._5)).distinct(), job_date).map(v => ((v._1, v._2), (v._3, v._4)))

    import sparkSession.implicits._
    val result_notification_finnal = result_notification_first_level.join(result_notification_second_level).map(v => ((v._1._1, v._1._2), (v._2._1._1, v._2._1._2, v._2._2._1, v._2._2._2))).join(result_notification_third_level).map(v => (v._1._1, v._1._2, v._2._1._1, v._2._1._2, v._2._1._3, v._2._1._4, v._2._2._1, v._2._2._2)).toDF("imei", "content", "pred_1", "label_1", "pred_2", "label_2", "pred_3", "label_3")
    result_notification_finnal.show(5)
    val save_table_name_thirdp = "algo.up_yf_shopping_interest_notification_result_all"
    val cols_thirdp = "imei string, content string, pred_1 string, label_1 string, pred_2 string, label_2 string, pred_3 string, label_3 string"
    save_result_to_hive(sparkSession, result_notification_finnal, cols_thirdp, save_table_name_thirdp, job_date)
  }

  def get_result_browser_all(sparkSession: SparkSession, labels: RDD[(String, String, String, String, String, String)], job_date: String) = {
    val result_notification_first_level_file = "/apps/recommend/models/wind/shopping_interrest/result/browser.first.level.result"
    val result_notification_second_level_file = "/apps/recommend/models/wind/shopping_interrest/result/browser.second.level.result"
    val result_notification_third_level_file = "/apps/recommend/models/wind/shopping_interrest/result/browser.third.level.result"
    print_sql(result_notification_first_level_file)
    print_sql(result_notification_second_level_file)
    print_sql(result_notification_third_level_file)

    val result_notification_first_level = get_result(sparkSession, result_notification_first_level_file, labels.map(v => (v._2, v._1)).distinct(), job_date).map(v => ((v._1, v._2), (v._3, v._4)))
    val result_notification_second_level = get_result(sparkSession, result_notification_second_level_file, labels.map(v => (v._4, v._3)).distinct(), job_date).map(v => ((v._1, v._2), (v._3, v._4)))
    val result_notification_third_level = get_result(sparkSession, result_notification_third_level_file, labels.map(v => (v._6, v._5)).distinct(), job_date).map(v => ((v._1, v._2), (v._3, v._4)))

    import sparkSession.implicits._
    val result_notification_finnal = result_notification_first_level.join(result_notification_second_level).map(v => ((v._1._1, v._1._2), (v._2._1._1, v._2._1._2, v._2._2._1, v._2._2._2))).join(result_notification_third_level).map(v => (v._1._1, v._1._2, v._2._1._1, v._2._1._2, v._2._1._3, v._2._1._4, v._2._2._1, v._2._2._2)).toDF("imei", "content", "pred_1", "label_1", "pred_2", "label_2", "pred_3", "label_3")
    result_notification_finnal.show(5)
    val save_table_name_thirdp = "algo.up_yf_shopping_interest_browser_result_all"
    val cols_thirdp = "imei string, content string, pred_1 string, label_1 string, pred_2 string, label_2 string, pred_3 string, label_3 string"
    save_result_to_hive(sparkSession, result_notification_finnal, cols_thirdp, save_table_name_thirdp, job_date)
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
