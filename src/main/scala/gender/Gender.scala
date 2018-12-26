package gender

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Gender {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181013"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Getting labeled data")
    val gender_labeled = get_gender_labeled_data(sparkSession)

    my_log("Getting data with features")
    val topK = 30000
    val feature_table: String = "algo.yf_user_behavior_features_app_install_on_" + topK.toString + "_dims"
    val (dataset_labeled, dataset_unlabeled) = get_dataset(sparkSession, gender_labeled, feature_table, topK, job_date)

    my_log("Building and trainning model")
    val class_num = 2
    val select_threshold_flag: Boolean = true
    val model_save_flag = true
    val model = build_model(sparkSession, dataset_labeled, class_num, select_threshold_flag, model_save_flag, job_date)

    my_log("Predicting")
    val prediction = predict(model, dataset_unlabeled)

    my_log("Union labelled and predicted data")
    var result = union_data(gender_labeled, prediction)

    my_log("Refine result with idcard data")
    val idcard_data = get_idcard_data(sparkSession, job_date)
    result = union_data(result, idcard_data)

    my_log("Save result")
    val save_table_name = "algo.yf_sex_model_app_install_predict_on_30000_new_1"
    val cols = "imei string, sex string"
    import sparkSession.implicits._
    save_result_to_hive(sparkSession, result.map(v => (v._1, if (v._2 == 1d) "male" else "female")).toDF("imei", "sex"), cols, save_table_name, job_date)
  }

  def get_gender_labeled_data(sparkSession: SparkSession) = {
    val select_know_sex_sql: String = "select imei, sex from algo.yf_imei_sex_for_build_sexmodel_flyme"
    print_sql(select_know_sex_sql)

    val data = sparkSession.sql(select_know_sex_sql).filter("imei is not null and sex is not null").rdd.map(v => (v(0).toString, v(1).toString.toDouble))
    printf("====>>>> gender labeled: %d\n", data.count())
    numerical_label_distribute(data)

    data
  }

  def get_idcard_data(sparkSession: SparkSession, yestoday_Date: String) = {
    val select_sql: String = "SELECT imei, gender from algo.yf_age_gender_accord_idcard_v2 where stat_date=" + yestoday_Date
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and gender is not null").rdd.map(v => (v(0).toString, v(1).toString)).map(v => (v._1, if (v._2 == "male") 1.0 else 0))
    numerical_label_distribute(data)
    data
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

  def build_model(
                   sparkSession: SparkSession,
                   labeled_data: RDD[(String, LabeledPoint)],
                   classes_num: Int,
                   select_threshold_flag: Boolean,
                   model_save_flag: Boolean,
                   yestoday_Date: String
                 ): LogisticRegressionModel = {
    val rdd_temp: Array[RDD[(String, LabeledPoint)]] = labeled_data.randomSplit(Array(0.8, 0.2))
    val train_rdd: RDD[(String, LabeledPoint)] = rdd_temp(0).cache()
    val valid_rdd: RDD[(String, LabeledPoint)] = rdd_temp(1).cache()
    printf("\n====>>>> trainset: %d\n", train_rdd.count())
    numerical_label_distribute(train_rdd.map(v => (v._1, v._2.label)))

    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(classes_num).run(train_rdd.map(_._2))

    println("\n\n****************** The Default threshold performance ***************************")
    val defalut_threshold: Double = model.getThreshold.get
    println("************* The Default threshold: " + defalut_threshold + "*****************")
    val valid_result_default_threshold: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    val ROC_default_thred = ROC(valid_result_default_threshold.map(_._2))
    val AUC_default_thred = ROC_default_thred._1
    val ACCU_default_thred = ROC_default_thred._2
    println("********************** AUROC_default_threshold: " + AUC_default_thred + " *********************")
    println("********************** Accuracy_default_threshold: " + ACCU_default_thred + " *********************\n\n")

    println("\n\n****************** The Best threshold performance ***************************")
    // choosing the best threshold
    model.clearThreshold()
    val valid_result_score: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    val binaryClassificationMetrics = new BinaryClassificationMetrics(valid_result_score.map(_._2))
    val fMeasure = binaryClassificationMetrics.fMeasureByThreshold()
    val best_threshold = fMeasure.reduce((a, b) => {
      if (a._2 < b._2) b else a
    })._1


    //    val threshold_set = 0.52
    //    if (best_threshold < threshold_set) {
    //      best_threshold = threshold_set
    //    }

    model.setThreshold(best_threshold)
    println("************* The Best threshold: " + model.getThreshold.get + "*****************")
    val valid_result_select_threshold: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    val ROC_select_thred = ROC(valid_result_select_threshold.map(_._2))
    val AUC_select_thred = ROC_select_thred._1
    val ACCU_select_thred = ROC_select_thred._2
    println("********************** AUROC_select_threshold: " + AUC_select_thred + " *********************")
    println("********************** Accuracy_select_threshold: " + ACCU_select_thred + " *********************\n\n")

    if (!select_threshold_flag) {
      model.setThreshold(defalut_threshold)
    }
    println("\n\n************** Finally Threshold: " + model.getThreshold.get + " ********************** \n\n")

    if (model_save_flag) {
      val model_save_path = "/apps/recommend/models/wind/gender"
      save_model(sparkSession, model, model_save_path)
    }
    model
  }

  def ROC(valid_result: RDD[(Double, Double)]): (Double, Double) = {
    val binary_class_metrics = new BinaryClassificationMetrics(valid_result)

    val roc = binary_class_metrics.roc()
    val au_roc = binary_class_metrics.areaUnderROC()
    val accuracy = valid_result.filter(v => v._1 == v._2).count() * 1.0 / valid_result.count()
    // println("\n\n ********************** AUROC: " + au_roc + " ********************* \n\n")
    return (au_roc, accuracy)
  }

  def predict(model: LogisticRegressionModel, dataset_unlabeled: RDD[(String, LabeledPoint)]): RDD[(String, Double)] = {
    val result = dataset_unlabeled.map(v => {
      val imei = v._1
      val pred_label = model.predict(v._2.features)
      (imei, pred_label)
    }).distinct()
    numerical_label_distribute(result)
    result
  }

  def union_data(data1: RDD[(String, Double)], data2: RDD[(String, Double)]): RDD[(String, Double)] = {
    val result = data1.union(data2).reduceByKey((a, b) => a)
    numerical_label_distribute(result)
    result
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
