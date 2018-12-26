package user_behavior_features

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Feature_app_install_new {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181013"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Getting topk apps")
    val topk = 30000
    val apps_info_table = "app_center.adl_sdt_adv_dim_app"
    val topk_save_table = "algo.up_yf_app_install_top" + topk.toString
    val topk_apps = get_topK_rdd(sparkSession, topk, apps_info_table, job_date, topk_save_table, save_flag = true)

    my_log("Getting app install features")
    val users_app_install_table = "app_center.adl_fdt_app_adv_model_install"
    val users_feat_save_table = "algo.yf_user_behavior_features_app_install_on_" + topk.toString + "_dims"
    get_user_feature_app_install(sparkSession, topk_apps, topk, users_app_install_table, job_date, users_feat_save_table, app_id_prefix_flag = true, save_flag = true)

    my_log("Getting app install features for push")
    val users_app_install_table_push = "push_service.ads_push_app_adv_model_install"
    val users_feat_save_table_push = "algo.yf_user_behavior_features_app_install_on_" + topk.toString + "_dims_push"
    get_user_feature_app_install(sparkSession, topk_apps, topk, users_app_install_table_push, job_date, users_feat_save_table_push, app_id_prefix_flag = false, save_flag = true)
  }

  def get_topK_rdd(sparkSession: SparkSession,
                   topK: Int,
                   data_table: String,
                   job_date: String,
                   save_table: String,
                   save_flag: Boolean): RDD[(String, String, Long, Long)] = {
    val select_sql: String = "select appid, app_name, installnum from " + data_table + " where stat_date=" + job_date
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("appid is not null and app_name is not null and installnum is not null").rdd.map(v => (v(0).toString, (v(1).toString, v(2).toString.toLong))).reduceByKey((a, b) => if (a._2 >= b._2) a else b).map(v => (v._2._2, (v._1, v._2._1)))

    // ("11"app_id, app_name, install_num, index)
    val data_ordered = data.sortBy(_._1, ascending = false).zipWithIndex().map(v => (v._1._2._1, v._1._2._2, v._1._1, v._2))
    printf("\n====>>>> apps_ordered_by_install_num: %d\n", data_ordered.count())

    val top_k_apps_rdd = sparkSession.sparkContext.parallelize(data.top(topK)).zipWithIndex().map(v => (v._1._2._1, v._1._2._2, v._1._1, v._2))
    printf("\n====>>>> tok_%d apps_ordered_by_install_num: %d\n", topK, top_k_apps_rdd.count())

    if (save_flag){
      import sparkSession.implicits._
      val top_k_apps_df = data_ordered.repartition(10).toDF("app_id", "app_name", "install_num", "index")
      val cols = "app_id string, app_name string, install_num bigint, index bigint"
      save_result_to_hive(sparkSession, top_k_apps_df, cols, save_table, job_date)
    }
    top_k_apps_rdd
  }

  def get_user_feature_app_install(sparkSession: SparkSession,
                                   top_k_apps: RDD[(String, String, Long, Long)],
                                   top_k: Int,
                                   data_table: String,
                                   job_date: String,
                                   save_table: String,
                                   app_id_prefix_flag: Boolean,
                                   save_flag: Boolean) = {
    printf("\n====>>>> use the top_%d apps to generate features", top_k)
    var apps_map = top_k_apps.map(v => (v._1, (v._2, v._4))).collectAsMap()

    if (app_id_prefix_flag) {
      printf("\n====>>>> using prefix: %s\n", "11")
      val temp = apps_map.map(v => ("11"+v._1, v._2))
      apps_map = temp
    }

    val select_sql: String = "select imei, value from " + data_table + " where stat_date=" + job_date
    printf("\n====>>>> %s\n", select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and value is not null").rdd.map(v => {
      val imei: String = v(0).toString
      val app_install_str = v(1).toString.split(" ")
      val app_install_array = ArrayBuffer[((String, String), Int)]()
      var imei_app_install_feat_array = ArrayBuffer[String]()
      for (item <- app_install_str) {
        val app_id: String = item.split(":")(0)
        if (apps_map.contains(app_id)) {
          val app_name = apps_map(app_id)._1
          val index = apps_map(app_id)._2.toString.toInt
          app_install_array += (((app_id, app_name), index))
        }
      }
      if (app_install_array.nonEmpty)
        (imei, app_install_array.sortBy(_._2))
      else
        (imei, app_install_array)
    })

    printf("\n====>>>> imei count(total): %d\n", data.count())

    val data_invalid = data.filter(_._2.isEmpty)
    printf("\n====>>>> imei count(app install list is empty): %d\n", data_invalid.count())

    val data_refined = data.filter(_._2.nonEmpty)
    printf("\n====>>>> imei count(refined by excluding empty list of app install): %d\n", data_refined.count())

    val data_refined_ordered = data_refined.map(v => (v._2.length, (v._1, v._2)))
    val min_len = data_refined_ordered.map(_._1).min()
    val max_len = data_refined_ordered.map(_._1).max()
    printf("\n====>>>> max/min app install num of imei: %d, %d\n", min_len, max_len)

//    val imei_features = data_refined.map(v => {
//      val imei = v._1
//      val feat = v._2.map(v => "[" + v._1._1 + "_" + v._1._2 + "]_wind_" + v._2.toString + ":1").mkString(" ")
//      (imei, feat)
//    })

    val imei_features = data_refined.map(v => {
      val imei = v._1
      val feat = v._2.map(v => v._1._2).mkString(" ")
      (imei, feat)
    })

    val imei_features_1 = data_refined.map(v => {
      val imei = v._1
      val feat = v._2.map(v => v._2.toString + ":1").mkString(" ")
      (imei, feat)
    })

    if (save_flag){
      import sparkSession.implicits._
      val imei_feat_df = imei_features.repartition(10).toDF("imei", "feature")
      val imei_feat_df_1 = imei_features_1.repartition(10).toDF("imei", "feature")
      val cols = "imei string, feature string"
      save_result_to_hive(sparkSession, imei_feat_df, cols, save_table+"_apps", job_date)
      save_result_to_hive(sparkSession, imei_feat_df_1, cols, save_table, job_date)
    }
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
