package shopping_interrest

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object User_Labeling_Model {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181104"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Getting user labels according a month user behavior")
    get_user_labels(sparkSession, job_date)

  }

  def get_user_labels(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select imei, labels, times, stat_date from algo.up_yf_shopping_interest_user_labels_record_daily where stat_date>" + offset_date(job_date, month_offset = -1, day_offset = 0)
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and labels is not null and times is not null and stat_date is not null").rdd.map(v =>(v(0).toString, v(1).toString, v(2).toString, v(3).toString))

    val data_refined = data.map(v => {
      val d = get_date_interval(v._4)
      val factor = (30 - d + 1) * 0.03333
      (v._1, v._2, v._3.toDouble * factor)
    }).map(v => (v._1, Array((v._2, v._3)))).reduceByKey(_++_).map(v => (v._1, v._2.sortBy(_._2).reverse.map(v => v._1 + ":" + v._2.formatted("%.3f").toString).mkString("###")))

    printf("\n====>>>> users: %d\n", data_refined.count())

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_shopping_interest_user_labels"
    val cols = "imei string, labels string"
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

  def get_date_interval(date: String) = {
    val date_format = new SimpleDateFormat("yyyyMMdd")
    val d = date_format.parse(date)
    val d_now = new Date()
    val interval_day = (d_now.getTime - d.getTime) / 1000 / 3600 / 24
    interval_day
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
