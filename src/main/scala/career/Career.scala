package career

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by lixiang on 2016/6/22.
  * get (imei,birthday) from flyme account table(user_center.mdl_flyme_users_info) and flymeid-imeiid table(user_profile.edl_uid_all_info)
  * transform (imei,birthday) to (imei,occupation) and store these in table(algo.lx_occupationPrediction_imei_occupation_known)
  */
object Career {

  def main(args: Array[String]): Unit = {
    // val job_time = "20180928"
    val job_time = args(0)

    my_log("Initial Spark")
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Get labeled data from flyme")
    val labeled_occupation_data = get_labeled_occupation_data(sparkSession, job_date)

    my_log("Filter labeled data for trainning")
    val labeled_occupation_data_refined = labeled_occupation_data.filter(v => v._2 != 7 && v._2 != 8 && v._2 != 9)
    numerical_label_distribute(labeled_occupation_data_refined)

    my_log("Geting all data with features including labeled and unlabeled set")
    val topK = 30000
    val feature_table: String = "algo.yf_user_behavior_features_app_install_on_" + topK.toString + "_dims"
    val (dataset_labeled, dataset_unlabeled) = get_dataset(sparkSession, labeled_occupation_data_refined, feature_table, topK, job_date)

    my_log("Building lr model and training")
    val model = build_model(sparkSession, dataset_labeled, dataset_unlabeled, classes_num = 7, model_save_flag = true, job_date)

    my_log("Predicting unlabeled data")
    val prediction = predict(model, dataset_unlabeled)

    my_log("Union labelled and predicted data")
    val result = union_train_and_prediction_data(labeled_occupation_data, prediction).reduceByKey((a, b) => a)

    my_log("Valid result")
    val result_refined = valid_result(sparkSession, result, job_date)
    val result_refined_count = result_refined.count()
    numerical_label_distribute(result_refined)

    val save_result_flag = true
    if (save_result_flag && result_refined_count > 1000) {
      my_log("Saving result to hive")
      val save_table_name = "algo.lx_occupation_appuse_prediction"
      val cols = "imei string, occupation int"
      import sparkSession.implicits._
      save_result_to_hive(sparkSession, result_refined.toDF("imei", "occupation"), cols, save_table_name, job_date)
    } else {
      printf("\n====>>>> the number of result is invalidate\n")
    }
  }

  def build_model(sparkSession: SparkSession,
                  dataset_labeled: RDD[(String, LabeledPoint)],
                  dataset_unlabeled: RDD[(String, LabeledPoint)],
                  classes_num: Int,
                  model_save_flag: Boolean,
                  job_date: String): LogisticRegressionModel = {
    val rdd_temp: Array[RDD[(String, LabeledPoint)]] = dataset_labeled.randomSplit(Array(0.8, 0.2))
    val train_rdd: RDD[(String, LabeledPoint)] = rdd_temp(0).cache()
    val valid_rdd: RDD[(String, LabeledPoint)] = rdd_temp(1).cache()
    printf("\n====>>>> trainset: %d\n", train_rdd.count())
    numerical_label_distribute(train_rdd.map(v => (v._1, v._2.label)))

    var model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(classes_num).run(train_rdd.map(_._2))
    val defalut_threshold: Double = model.getThreshold.get
    printf("\n====>>>> default threshold: %.2f", defalut_threshold)

    val valid_result: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_occupation_valid_result"
    val cols = "imei string, pred double, label double"
    val valid_result_df = valid_result.map(v => (v._1, v._2._1, v._2._2)).toDF("imei", "pred", "label")
    save_result_to_hive(sparkSession, valid_result_df, cols, save_table_name, job_date)
    confusion_matrix(valid_result.map(_._2))

    if (model_save_flag) {
      val model_save_path = "/apps/recommend/models/wind/career"
      save_model(sparkSession, model, model_save_path)
    }

    model
  }

  def predict(model: LogisticRegressionModel, dataset_unlabeled: RDD[(String, LabeledPoint)]): RDD[(String, Double)] = {
    val result = dataset_unlabeled.map(v => {
      val imei = v._1
      val pred_label = model.predict(v._2.features)
      (imei, pred_label)
    })
    numerical_label_distribute(result)
    result
  }

  def valid_result(sparkSession: SparkSession, raw_result: RDD[(String, Double)], job_date: String) = {
    val occupation_silent = get_imei_occupation_silent(sparkSession, job_date)
    var refined = raw_result.union(occupation_silent).reduceByKey((a, b) => if(a > b) a else  b)

    val occupation_student = get_imei_occupation_student(sparkSession, job_date)
    refined = refined.union(occupation_student).reduceByKey((a, b) => if(a == 0) a else b )

    val occupation_it = get_imei_occupation_IT(sparkSession, job_date)
    refined = refined.union(occupation_it).reduceByKey((a, b) => if(a == 8) a else b)

    val select_age_sql = "select imei, age from algo.up_yf_age where stat_date=" + job_date
    print_sql(select_age_sql)
    val age_date = sparkSession.sql(select_age_sql).rdd.map(v => (v(0).toString, v(1).toString.toDouble))

    refined = refined.join(age_date).map(v => {
      val imei = v._1
      val age = v._2._2
      var occup = v._2._1
      if(age == 0 || age == 1)
        occup = 0
      if((occup == 0 && age == 3) || (occup == 0 && age == 4))
        occup = 10
      (imei, occup)
    })

    numerical_label_distribute(refined)

    refined
  }

  def get_imei_occupation_silent(sparkSession: SparkSession, yestoday_Date: String): RDD[(String, Double)] = {
    val select_imei_app_install_sql = "select imei from app_center.adl_fdt_app_adv_model_install where stat_date = "+yestoday_Date
    val select_imei_app_use_sql = "select imei from app_center.adl_fdt_app_adv_model_boot where stat_date = "+yestoday_Date

    val install_item_df: DataFrame = sparkSession.sql(select_imei_app_install_sql)
    val install_imei_lable = install_item_df.rdd.map(r => r.get(0).toString.trim)

    val use_item_df: DataFrame = sparkSession.sql(select_imei_app_use_sql)
    //(imei,label)
    val use_imei_lable = use_item_df.rdd.map(r => r.get(0).toString.trim)

    //    val imei_occupation_silent_temp:RDD[(String, Double)] = install_imei_lable.union(use_imei_lable).reduceByKey((a, b) => (a))
    val imei_occupation_silent = install_imei_lable.subtract(use_imei_lable).map(r => {
      val label: Double = 10d//
      (r,label)
    })

    imei_occupation_silent
  }

  def union_train_and_prediction_data(trainset: RDD[(String, Double)], predictset: RDD[(String, Double)]): RDD[(String, Double)] = {
    val result = trainset.union(predictset)
    numerical_label_distribute(result)
    result
  }

  def get_labeled_occupation_data(sparkSession: SparkSession, job_date: String): RDD[(String, Double)] = {
    val select_imei_occupation_sql = "select b.imei, a.occupation from (select user_id,occupation from user_profile.dwd_user_uid_basic where stat_date = " + job_date + " and length(occupation) > 0) a join (select imei, uid from user_profile.dwd_user_uid_basic_ext where stat_date = " + job_date + ") b on a.user_id = b.uid"
    print_sql(select_imei_occupation_sql)

    val item_df: DataFrame = sparkSession.sql(select_imei_occupation_sql)
    //(imei,label)
    val rdd_imei_lable: RDD[(String, Double)] = item_df.rdd.map(r => {
      var label: Int = 0
      val imei = r.get(0).toString.trim
      val occupation = r.get(1).toString.trim

      if (occupation.compareToIgnoreCase("学生") == 0)
        label = 1
      else if (occupation.compareToIgnoreCase("建筑") == 0)
        label = 8
      else if (occupation.compareToIgnoreCase("法律") == 0) //b
        label = 6
      else if (occupation.compareToIgnoreCase("艺术") == 0) //c
        label = 7
      else if (occupation.compareToIgnoreCase("文化") == 0) //c
        label = 5
      else if (occupation.compareToIgnoreCase("IT") == 0)
        label = 9
      else if (occupation.compareToIgnoreCase("商业") == 0) //a
        label = 10
      else if (occupation.compareToIgnoreCase("行政") == 0) //c
        label = 4
      else if (occupation.compareToIgnoreCase("金融") == 0) //a
        label = 2
      else if (occupation.compareToIgnoreCase("医疗") == 0)
        label = 3
      (imei, label.toDouble)
    }).filter(_._2 != 0).map(v => (v._1, v._2 - 1))
    numerical_label_distribute(rdd_imei_lable)

    rdd_imei_lable
  }

  def get_dataset(sparkSession: SparkSession, labeled_dataset: RDD[(String, Double)], imei_features_table: String, feature_dim: Int, job_date: String): (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]) = {
    val latest_date = get_latest_date(sparkSession, imei_features_table)
    val select_features_sql: String = "select imei, feature from " + imei_features_table + " where stat_date=" + latest_date
    printf("\n====>>>> %s\n", select_features_sql)
    val imei_features_df = sparkSession.sql(select_features_sql).filter("imei is not null and feature is not null")

    val dataset: RDD[(String, String, Double)] = imei_features_df.rdd.map(v => (v(0).toString, v(1).toString)).leftOuterJoin(labeled_dataset).map(v => (v._1, v._2._1, v._2._2.getOrElse(-1)))

    val dataset_label_point = dataset.map(v => {
      val imei = v._1
      val features_array = v._2.trim.split(" ")
      val index_array = new ArrayBuffer[Int]()
      val value_array = new ArrayBuffer[Double]()

      for (item <- features_array){
        val index_value = item.trim.split(":")
        if (index_value.length == 2){
          index_array += index_value(0).trim.toInt
          value_array += index_value(1).trim.toDouble
        }
      }
      (imei, index_array, value_array, v._3.toDouble)
    }).filter(v => v._2.nonEmpty && v._3.nonEmpty).map(v => (v._1, new LabeledPoint(v._4, Vectors.sparse(feature_dim, v._2.toArray, v._3.toArray))))

    val dataset_labeled = dataset_label_point.filter(_._2.label != -1)
    val dataset_unlabeled = dataset_label_point.filter(_._2.label == -1)
    printf("\n====>>>> dataset_labeled: %d\n", dataset_labeled.count())
    printf("\n====>>>> dataset_unlabeled: %d\n", dataset_unlabeled.count())

    (dataset_labeled, dataset_unlabeled)
  }

  def get_imei_occupation_IT(sparkSession: SparkSession, yestoday_Date: String): RDD[(String, Double)] = {
    val select_imei_app_install_sql = "select imei from app_center.adl_fdt_app_adv_model_install where stat_date = " + yestoday_Date + " and value like \"%2019090%\""
    val select_imei_app_use_sql = "select imei from app_center.adl_fdt_app_adv_model_boot where stat_date = " + yestoday_Date + " and value like \"%2019090%\"" //dingding
    print_sql(select_imei_app_install_sql)
    print_sql(select_imei_app_use_sql)

    val install_item_df: DataFrame = sparkSession.sql(select_imei_app_install_sql)
    val install_imei_lable: RDD[(String, Double)] = install_item_df.rdd.map(r => {
      val label: Double = 8d
      //IT
      val imei = r.get(0).toString.trim
      (imei, label)
    })

    val use_item_df: DataFrame = sparkSession.sql(select_imei_app_use_sql)
    //(imei,label)
    val use_imei_lable: RDD[(String, Double)] = use_item_df.rdd.map(r => {
      val label: Double = 8d
      //IT
      val imei = r.get(0).toString.trim
      (imei, label)
    })

    val imei_occupation_IT: RDD[(String, Double)] = install_imei_lable.union(use_imei_lable).reduceByKey((a, b) => (a))

    imei_occupation_IT
  }

  def get_imei_occupation_student(sparkSession: SparkSession, yestoday_Date: String): RDD[(String, Double)] = {
    val select_imei_app_install_sql = "select imei from app_center.adl_fdt_app_adv_model_install where stat_date = " + yestoday_Date + " and (value like \"%1784004%\" or value like \"%1914584%\" or value like \"%546122%\")"
    val select_imei_app_use_sql = "select imei from app_center.adl_fdt_app_adv_model_boot where stat_date = " + yestoday_Date + " and (value like \"%1784004%\" or value like \"%1914584%\" or value like \"%546122%\")" //
    print_sql(select_imei_app_install_sql)
    print_sql(select_imei_app_use_sql)

    val install_item_df: DataFrame = sparkSession.sql(select_imei_app_install_sql)
    val install_imei_lable: RDD[(String, Double)] = install_item_df.rdd.map(r => {
      val label: Double = 0d
      //
      val imei = r.get(0).toString.trim
      (imei, label)
    })

    val use_item_df: DataFrame = sparkSession.sql(select_imei_app_use_sql)
    //(imei,label)
    val use_imei_lable: RDD[(String, Double)] = use_item_df.rdd.map(r => {
      val label: Double = 0d
      //
      val imei = r.get(0).toString.trim
      (imei, label)
    })

    val imei_occupation_student: RDD[(String, Double)] = install_imei_lable.union(use_imei_lable).reduceByKey((a, b) => (a))

    imei_occupation_student
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
    labeled_data.map(v => (v._2, 1)).reduceByKey(_+_).collect().sortBy(_._1).foreach(f = v => {
      printf("\n====>>>> label_%s: %d, all: %d, ratio: %.4f\n", v._1, v._2, labeled_data_count, v._2 * 1.0 / labeled_data_count)
    })
  }

  def confusion_matrix(valid_result: RDD[(Double, Double)]) = {
    val multiclassMetrics = new MulticlassMetrics(valid_result)
    printf("\n====>>>> overall Accuracy: %.4f\n", multiclassMetrics.accuracy)

    for (i <- multiclassMetrics.labels) {
      printf("\n====>>>> Precision: %.4f, Recall: %.4f, FMeature: %.4f\n", multiclassMetrics.precision(i), multiclassMetrics.recall(i), multiclassMetrics.fMeasure(i))
    }
  }

  def get_latest_date(sparkSession: SparkSession, table_name: String) = {
    val partitions = sparkSession.sql(s"show partitions $table_name").rdd.map(r=>r.getAs[String](0)).collect()
    val lastStatDate = partitions.sortWith((a,b)=>a>b).apply(0).split("=")(1)
    printf("\n====>>>> lastest date of %s: %s\n", table_name, lastStatDate)
    lastStatDate
  }

  def save_model(sparkSession: SparkSession, model: LogisticRegressionModel, save_dir: String) = {
    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val p = new Path(save_dir)
    if (fs.exists(p)) {
      printf("\n====>>>> model file is alread exists! delete it! %s", p.toString)
      fs.delete(p, true)
    }
    printf("\n====>>>> save model to %s", p.toString)
    model.save(sparkSession.sparkContext, p.toString)
  }
}
