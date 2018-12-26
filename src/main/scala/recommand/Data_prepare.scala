package recommand

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Data_prepare {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181013"
    val (sparkSession, job_date) = init_job(job_time)

//    my_log("Get items")
//    val items = get_items(sparkSession, job_date)

    val event_score_map:Map[String, Double] = Map("content_click"->1.0, "content_comment"->2.0, "content_like"->3.0, "content_share"->5.0, "content_danmku"->2.0, "browse_progress"->0.0, "video_play"->0.0)

//    my_log("User actions daily")
//    get_user_actions_daily(sparkSession, event_score_map, job_date)

//    my_log("User actions")
//    get_user_actions(sparkSession, event_score_map: Map[String, Double], job_date)

    my_log("User item rating")
    get_user_item_rating(sparkSession, event_score_map, job_date)
  }

  def get_user_item_rating(sparkSession: SparkSession, event_score_map: Map[String, Double], job_date: String) = {
    val select_sql = "select imei, item_id, action, frequency from algo.recomm_yf_user_actions where stat_date=20181023"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and item_id is not null and action is not null and frequency is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v(3).toString.toDouble)).map(v => ((v._1, v._2), event_score_map(v._3) * v._4)).reduceByKey(_+_).map(v => (v._1._1, v._1._2, v._2))
    printf("====>>>> actions count: %d", data.count())
    printf("====>>>> validation: %d", data.map(v => (v._1, v._2)).distinct().count())

    import sparkSession.implicits._
    val cols = "imei string, item_id string, rating double"
    val save_table = "algo.recomm_yf_user_item_rating"
    save_result_to_hive(sparkSession, data.toDF(), cols, save_table, job_date)

  }

  def get_user_actions(sparkSession: SparkSession, event_score_map: Map[String, Double], job_date: String) = {
    val select_sql = "select imei, item_id, action, frequency from algo.recomm_yf_user_actions_daily where stat_date > 20180501 and stat_date < 20180515"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and item_id is not null and action is not null and frequency is not null").rdd.map(v => ((v(0).toString, v(1).toString, v(2).toString), v(3).toString.toDouble)).reduceByKey(_+_).map(v => (v._1._1, v._1._2, v._1._3, v._2)).filter(_._1.length > 5)
    printf("====>>>> actions count: %d", data.count())

    import sparkSession.implicits._
    val cols = "imei string, item_id string, action string, frequency double"
    val save_table = "algo.recomm_yf_user_actions"
    save_result_to_hive(sparkSession, data.toDF(), cols, save_table, job_date)
  }

  def get_user_actions_daily(sparkSession: SparkSession, event_score_map: Map[String, Double], job_date: String) = {
    val select_sql = "select platform['_imei_'] as imei, event_values['content_id'] as item_id, event_infos['_name_'] as action from mzacgn.ods_any_uxip_acgfun where stat_date like '" + job_date + "%'"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and item_id is not null and action is not null")
    data.show(2)

    val data_rdd = data.rdd.map(v => (v(0).toString, v(1).toString, v(2).toString)).filter(v => event_score_map.contains(v._3)).map(v => ((v._1, v._2, v._3), 1)).reduceByKey(_+_).map(v => (v._1._1, v._1._2, v._1._3, v._2))
    printf("====>>>> actions count: %d", data_rdd.count())

    import sparkSession.implicits._
    val cols = "imei string, item_id string, action string, frequency double "
    val save_table = "algo.recomm_yf_user_actions_daily"
    save_result_to_hive(sparkSession, data_rdd.toDF(), cols, save_table, job_date)
  }

  def get_items(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "SELECT content_id, title, content, author_id, author, tag from mzacgn.ods_any_acgn_content where stat_date>='2018050100'"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("content_id is not null and title is not null and content is not null and author_id is not null and author is not null and tag is not null")
    data.show(2)
    val data_rdd = data.rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v(3).toString, v(4).toString, v(5).toString)).distinct()
    printf("====>>>> items count: %d", data_rdd.count())

    import sparkSession.implicits._
    val cols = "item_id string, title string, content string, author_id string, author_name string, tag string"
    val save_table = "algo.recomm_yf_items"
    save_result_to_hive(sparkSession, data_rdd.toDF(), cols, save_table, job_date)
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

  def get_latest_date(sparkSession: SparkSession, table_name: String) = {
    val partitions = sparkSession.sql(s"show partitions $table_name").rdd.map(r=>r.getAs[String](0)).collect()
    val lastStatDate = partitions.sortWith((a,b)=>a>b).apply(0).split("=")(1)
    printf("\n====>>>> lastest date of %s: %s\n", table_name, lastStatDate)
    lastStatDate
  }
}
