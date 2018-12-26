package temp_test


import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object MyTest_1 {
  def main(args: Array[String]): Unit = {
    val job_time = "20180913"
    val (sparkSession, job_date) = init_job(job_time)

    val select_sql = "SELECT imei,search as SearchData,app,match_kw as keyword from mzreader.ods_stream_search_action_overall where stat_date like '20180913%' and app in ('com.taobao.taobao', 'com.xunmeng.pinduoduo', 'com.jingdong.app.mall', 'com.achievo.vipshop', 'com.xingin.xhs', 'com.tmall.wireless')"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql)

    printf("\n====>>>> data count: %d \n", data.count())
    data.show(5)
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

  def print_sql(ss: String) = {
    printf("\n====>>>> %s\n", ss)
  }
}
