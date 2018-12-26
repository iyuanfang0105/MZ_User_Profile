package shopping_interrest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object User_Labeling_Model_Daily {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181104"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("User label recording daily")
    get_user_labels_record(sparkSession, job_date)
  }

  def get_user_labels_record(sparkSession: SparkSession, job_date: String) = {
    val select_sql_notification = "select imei, pred_3, label_3 from algo.up_yf_shopping_interest_notification_result_all where stat_date=" + job_date
    print_sql(select_sql_notification)
    val data_notifaction = sparkSession.sql(select_sql_notification).filter("imei is not null and pred_3 is not null and label_3 is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString))

    val select_sql_3d_commerce = "select fmd5, pred_3, label_3 from algo.up_yf_shopping_interest_3d_ecommerce_result_all where stat_date=" + job_date
    print_sql(select_sql_3d_commerce)
    val data_3d_commerce = sparkSession.sql(select_sql_3d_commerce).filter("fmd5 is not null and pred_3 is not null and label_3 is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString))

    val select_sql_browser = "select imei, pred_3, label_3 from algo.up_yf_shopping_interest_browser_result_all where stat_date=" + job_date
    print_sql(select_sql_browser)
    val data_browser = sparkSession.sql(select_sql_browser).filter("imei is not null and pred_3 is not null and label_3 is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString))

    val select_sql_search = "select imei, pred3, label_3 from algo.up_wy_shopping_interest_search_result where stat_date=" + job_date
    print_sql(select_sql_search)
    val data_search = sparkSession.sql(select_sql_search).filter("imei is not null and pred3 is not null and label_3 is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString))

    val data = data_notifaction.union(data_3d_commerce).union(data_browser).union(data_search)
    val data_refined = data.map(v => (v, 1)).reduceByKey(_+_).map(v => (v._1._1, v._1._2, v._1._3, v._2)).map(v => (v._1, v._2 + "@" + v._3, v._4))

    // val data_refined = data.map(v => (v, 1)).reduceByKey(_+_).map(v => (v._1._1, v._1._2, v._1._3, v._2)).map(v => (v._1, Array((v._2 + "@" + v._3, v._4)))).reduceByKey(_++_).map(v => (v._1, v._2.sortBy(_._2).reverse.map(v => v._1 + ":" + v._2.toString).mkString("###")))
    // val data_refined = data.map(v => (v, 1)).reduceByKey(_+_).map(v => (v._1._1, v._1._2,  v._2)).map(v => (v._1, Array((v._2, v._3)))).reduceByKey(_++_).map(v => (v._1, v._2.sortBy(_._2).reverse.map(v => v._1 + ":" + v._2.toString).mkString(" ")))

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_shopping_interest_user_labels_record_daily"
    val cols = "imei string, labels string, times int"
    save_result_to_hive(sparkSession, data_refined.toDF(), cols, save_table_name, job_date)
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
}
