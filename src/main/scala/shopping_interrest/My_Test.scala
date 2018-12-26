package shopping_interrest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD


object My_Test {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181010"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Getting fdm5_imei")
    val fdm5_imei = get_imei_fdm5(sparkSession, job_date)

    my_log("Prepare data")
    prepare_third_party_ecommerce_data(sparkSession, fdm5_imei, job_date)


  }


  def prepare_third_party_ecommerce_data(sparkSession: SparkSession, fdm5_imei: RDD[(String, String)], job_date: String) = {
    val select_sql = "SELECT fmd5, furl, ftitle, fkeywords, fcontent, fdescription, src_type from uxip.dwd_browser_url_creeper where stat_date=" + job_date+ " and src_type in ('com.taobao.taobao', 'com.xunmeng.pinduoduo', 'com.jingdong.app.mall', 'com.achievo.vipshop', 'com.xingin.xhs', 'com.tmall.wireless')"
    print_sql(select_sql)
    val data = sparkSession.sql(select_sql).filter("fmd5 is not null and furl is not null and ftitle is not null and fcontent is not null and fdescription is not null and src_type is not null").distinct().rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v.getString(3), v(4).toString, v(5).toString, v(6).toString))
    val data_count = data.count()
    printf("\n====>>>> the raw data of third-party commerce: %d\n", data_count)

    val data_refined = data.map(v => (v._1, v._2)).join(fdm5_imei)
    printf("\n====>>>> data refined: %d\n", data_refined.count())
  }

  def get_imei_fdm5(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select url_md5, imei from uxip.ods_userprofile_url where stat_date=" + job_date
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("url_md5 is not null and imei is not null").rdd.map(v => (v(0).toString, v(1).toString)).filter(v => v._1.length > 1 && v._2.length > 1).distinct()
    printf("\n====>>>> fdm5-imei: %d\n", data.count())
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

  def numerical_label_distribute(labeled_data: RDD[(String, Double)]) = {
    val labeled_data_count = labeled_data.count()
    labeled_data.map(v => (v._2, 1)).reduceByKey(_+_).collect().sortBy(_._1).foreach(f = v => {
      printf("\n====>>>> label_%s: %d, all: %d, ratio: %.4f\n", v._1, v._2, labeled_data_count, v._2 * 1.0 / labeled_data_count)
    })
  }

}
