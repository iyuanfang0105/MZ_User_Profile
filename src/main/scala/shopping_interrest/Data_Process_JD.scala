package shopping_interrest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.recognition.impl.StopRecognition
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object Data_Process_JD {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20180810"
    val first_l = args(1)
    val second_l = args(2)
    val third_l = args(3)

    val (sparkSession, job_date) = init_job(job_time)

    my_log("Prepare jd data")
    val data = prepare_dj_data_for_train(sparkSession)

    my_log("Getting labels info")
    val first_level_label = get_labels(first_l)
    val second_level_label = get_labels(second_l)
    val third_level_label = get_labels(third_l)
    import sparkSession.implicits._
    val data_extend = data.map(v => (v._1, v._2, v._3, v._4, first_level_label(v._4)._1.toString + "-" + first_level_label(v._4)._2.toString, v._5, second_level_label(v._5)._1.toString + "-" + second_level_label(v._5)._2.toString, v._6, third_level_label(v._6)._1.toString + "-" + third_level_label(v._6)._2.toString)).toDF("id", "title", "desc", "category_1", "label_1", "category_2", "label_2", "category_3", "label_3")
    data_extend.show(10)

    my_log("Save to hive")
    val cols = "id string, title string, desc string, category_1 string, label_1 string, category_2 string, label_2 string, category_3 string, label_3 string"
    val save_table_name = "algo.up_yf_shopping_interest_jd_labelled_data_1"
    save_result_to_hive(sparkSession, data_extend, cols, save_table_name, job_date="")
  }

  def prepare_dj_data_for_train(sparkSession: SparkSession) = {
    val select_sql = "select fid, ftitle, fcategory, fattributes from mzmall.ods_t_galaxy_jd_c"
    print_sql(select_sql)
    val data = sparkSession.sql(select_sql).filter("fid is not null and ftitle is not null and fcategory is not null").distinct()
    printf("\n====>>>> raw data count: %d\n", data.count())

    val data_refine = data.rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v(3).toString)).map(v => (v._1, v._2, v._4, v._3.trim.split("""\|"""))).filter(_._4.length == 3).map(v => (v._1, v._2, v._3, v._4(0), v._4(0) + "-" + v._4(1), v._4.mkString("-")))
    printf("\n====>>>> refined data count: %d\n", data_refine.count())

    data_refine.map(v => (v._1, segment(v._2), segment(v._3), v._4, v._5, v._6))
  }

  def segment(s: String) = {
    val sp_filter = new StopRecognition()
    sp_filter.insertStopNatures("w")

    val split_s = ToAnalysis.parse(s).recognition(sp_filter).toStringWithOutNature(" ")
    split_s
  }

  def get_labels(label_path: String) = {
    var label: Map[String, (Int, Int)] = Map()
    for (line <- Source.fromFile(label_path).getLines()) {
      val tmp = line.split("\t")
      if (tmp.length == 3) {
        label += (tmp(0) -> (tmp(1).toInt, tmp(2).toInt))
      }
    }
    label
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
}
