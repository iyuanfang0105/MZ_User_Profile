package shopping_interrest.fasttext

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Labeled_Data_JD {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    // val job_time = args(0)
    val job_time = "20180810"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Get JD labeled data")
    get_jd_data(sparkSession)
  }

  def get_jd_data(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val select_sql = "SELECT id, title, desc, category_1, label_1, category_2, label_2, category_3, label_3 from algo.up_yf_shopping_interest_jd_labelled_data_1"
    print_sql(select_sql)
    val data = sparkSession.sql(select_sql).filter("id is not null and title is not null and desc is not null and category_1 is not null and label_1 is not null and category_2 is not null and label_2 is not null and category_3 is not null and label_3 is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v(3).toString, v(4).toString, v(5).toString, v(6).toString, v(7).toString, v(8).toString)).distinct()

    printf("\n====>>>> jd labeled data count: %d", data.count())

    val data_first_level = data.map(v => {
      val tmp = v._5.split("-")
      var count = -1
      var label = -1
      if (tmp.length == 2){
        count = tmp(0).toInt
        label = tmp(1).toInt
      }
      (v._2, count, label)
    }).filter(v => v._2 != -1 && v._3 != -1).filter(_._2 >= 10000)
    printf("\n====>>>> first level: %d", data_first_level.map(_._3).distinct().count())

    numerical_label_distribute(data_first_level.map(v => (v._1, v._3)))
    val data_first_level_refined = data_first_level.map(v => "__label__" + v._3.toString + ", " + v._1)
    data_first_level_refined.toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/shopping_interrest/labeled_jd_data_first_level")
    // data_first_level_refined.coalesce(1, true).saveAsTextFile("/apps/recommend/models/wind/shopping_interrest/labeled_jd_data_first_level")

    val data_second_level = data.map(v => {
      val tmp = v._7.split("-")
      var count:Int = -1
      var label:Int = -1
      if (tmp.length == 2){
        count = tmp(0).toInt
        label = tmp(1).toInt
      }
      (v._2, count, label)
    }).filter(v => v._2 != -1 && v._3 != -1).filter(_._2 >= 10000)
    printf("\n====>>>> second level: %d", data_second_level.map(_._3).distinct().count())

    numerical_label_distribute(data_second_level.map(v => (v._1, v._3)))
    val data_second_level_refined = data_second_level.map(v => "__label__" + v._3.toString + ", " + v._1)
    data_second_level_refined.toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/shopping_interrest/labeled_jd_data_second_level")
    //data_second_level_refined.coalesce(1, true).saveAsTextFile("/apps/recommend/models/wind/shopping_interrest/labeled_jd_data_second_level")

    val data_third_level = data.map(v => {
      val tmp = v._9.split("-")
      var count:Int = -1
      var label:Int = -1
      if (tmp.length == 2){
        count = tmp(0).toInt
        label = tmp(1).toInt
      }
      (v._2, count, label)
    }).filter(v => v._2 != -1 && v._3 != -1).filter(_._2 >= 10000)
    printf("\n====>>>> third level: %d", data_third_level.map(_._3).distinct().count())

    numerical_label_distribute(data_third_level.map(v => (v._1, v._3)))
    val data_third_level_refined = data_third_level.map(v => "__label__" + v._3.toString + ", " + v._1)
    data_third_level_refined.toDF().repartition(10).write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/shopping_interrest/labeled_jd_data_third_level")
    //data_third_level_refined.coalesce(1, true).saveAsTextFile("/apps/recommend/models/wind/shopping_interrest/labeled_jd_data_third_level")
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

  def save_data_2_hdfs(sc: SparkSession, output_dir: String, result: RDD[(String, Long)]): Unit = {
    val data = result.map {
      case (user, items) =>
        s"$user#$items"
    }
    //    val hdfsRootDir = output_dir
    val outputDir = output_dir
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val outputPath = new org.apache.hadoop.fs.Path(outputDir)
    if (hdfs.exists(outputPath)) hdfs.delete(outputPath, true)

    data.saveAsTextFile(outputDir)
  }

  def numerical_label_distribute(labeled_data: RDD[(String, Double)]) = {
    val labeled_data_count = labeled_data.count()
    labeled_data.map(v => (v._2, 1)).reduceByKey(_+_).collect().sortBy(_._2).foreach(f = v => {
      printf("\n====>>>> label_%s: %d, all: %d, ratio: %.4f\n", v._1, v._2, labeled_data_count, v._2 * 1.0 / labeled_data_count)
    })
  }
}
