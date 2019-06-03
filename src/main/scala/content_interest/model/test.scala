package content_interest.model

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object test {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181118"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Prepare trainning data")
    get_trainning_data(sparkSession, month_offset = -18, job_date)
  }

  def get_trainning_data(sparkSession: SparkSession, month_offset: Int, job_date: String): Unit = {
    // got data in a duration
    val select_sql = "select ftitle, fcontent, fcategory, fcategory_id, level from algo.up_yf_content_interest_trainning_data where stat_date>=" + offset_date(job_date, month_offset = month_offset, day_offset = 0) + " and fsource='UC'"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("fid is not null and ftitle is not null and fcontent is not null and fcategory is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v(3).toString, v(4).toString.toInt))
//    data.take(10).foreach(println)

    printf("\n====>>>>train data of %d months: %d\n", month_offset, data.count())

    import sparkSession.implicits._

    data.filter(_._5 == 1).map(v => "__label__" + v._3 + "_" + v._4.toString + ", " + v._1).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/labelled_data/title_first_level")
    data.filter(_._5 == 2).map(v => "__label__" + v._3 + "_" + v._4.toString + ", " + v._1).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/labelled_data/title_second_level")
    data.filter(_._5 == 1).map(v => "__label__" + v._3 + "_" + v._4.toString + ", " + v._2).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/labelled_data/content_first_level")
    data.filter(_._5 == 2).map(v => "__label__" + v._3 + "_" + v._4.toString + ", " + v._2).toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/content_interrest/labelled_data/content_second_level")
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
