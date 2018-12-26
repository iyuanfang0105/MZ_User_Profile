package shopping_interrest.fasttext

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object Data_For_Infer {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181013"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Get third party commerce data and write to hdfs")
    get_third_party_ecommerce(sparkSession, job_date)

    my_log("Get notification data and write to hdfs")
    get_notification(sparkSession, job_date)

    my_log("Get browser data and write to hdfs")
    get_browser_data(sparkSession, job_date)
  }

  def get_third_party_ecommerce(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select fmd5, title from algo.up_yf_shopping_interest_3d_ecommerce where stat_date=" + job_date
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("fmd5 is not null and title is not null").rdd.map(v => v(0).toString + "##yf##" + v(1).toString).distinct()

//    val dir_hdfs = "/apps/recommend/models/wind/shopping_interrest/third_party_commerce"
//    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
//    if (fs.exists(new Path(dir_hdfs)))
//      fs.delete(new Path(dir_hdfs), true)
//    data.coalesce(1, true).saveAsTextFile(dir_hdfs)
    import sparkSession.implicits._
    data.toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/shopping_interrest/third_party_commerce")
  }

  def get_notification(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select imei, content from algo.up_yf_shopping_interest_notification where stat_date=" + job_date
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and content is not null").rdd.map(v => v(0).toString + "##yf##" + v(1).toString).distinct()

    import sparkSession.implicits._
    data.toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/shopping_interrest/notification")
  }

  def get_browser_data(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select fid, ftitle from algo.up_yf_shopping_interest_browser where stat_date=" + job_date + " and domain='item.m.jd.com'"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("fid is not null and ftitle is not null").rdd.map(v => (v(0).toString, v(1).toString)).filter(_._2.length >= 15).map(v => v._1 + "##yf##" + v._2).distinct()

    import sparkSession.implicits._
    data.toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/shopping_interrest/browser")
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
