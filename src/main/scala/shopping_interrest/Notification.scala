package shopping_interrest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Notification {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20180810"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Get notification_data")
    val notification_table = "algo.up_yf_shopping_interest_notification"
    val notification_data = get_notification_data(sparkSession, notification_table, job_date)
  }

  def get_notification_data(sparkSession: SparkSession, table_name: String, job_date: String) = {
    val sources = Array("com.taobao.taobao", "com.xunmeng.pinduoduo", "com.jingdong.app.mall", "com.achievo.vipshop", "com.xingin.xhs", "com.tmall.wireless")
    var source_str = ""
    for (s <- sources) {
      source_str = source_str + "'" + s + "'" + ","
    }
    source_str = source_str.stripSuffix(",")

    val select_sql = "select imei, title, content, source, event_name from " + table_name + " where stat_date=" + job_date + " and source in (" + source_str + ")"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and title is not null and content is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v(3).toString, v(4).toString)).filter(v => v._2.length > 0 && v._3.length > 0)
    val data_count = data.count()
    printf("\n====>>>> notification data: %d\n", data_count)

    printf("\n\n\n")
    data.map(v => (v._4, 1)).reduceByKey(_+_).collect().foreach(v => {
      printf("\n====>>>> %s: %d, all: %d, ratio: %.4f\n", v._1, v._2, data_count, v._2 * 1.0 / data_count)
    })

    printf("\n\n\n")
    data.map(v => (v._5, 1)).reduceByKey(_+_).collect().foreach(v => {
      printf("\n====>>>> %s: %d\n", v._1, v._2)
    })

    printf("\n\n\n")
    for (s <- sources) {
      data.filter(_._4 == s).map(v => (v._5, 1)).reduceByKey(_+_).collect().foreach(v => {
        printf("\n====>>>> %s-%s: %d\n", s, v._1, v._2)
      })
    }

    data
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

}
