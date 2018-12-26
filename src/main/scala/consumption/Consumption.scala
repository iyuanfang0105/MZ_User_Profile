package consumption

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Consumption {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark", level = 1)
    val job_time = args(0)
    // val job_time = "20181201"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Prepare data", level = 1)
    val user_behavoir_features_table_name: String = "algo.yf_user_behavior_features_app_install_on_30000_dims"
    val questionnaire_table_name: String = "algo.yf_questionnaire_data_new"
    val labeled_data = get_consumption_label_from_questionnaire(sparkSession, questionnaire_table_name)

    val features_dim: Int = 30000
    val dataset = get_data_set_for_build_model(sparkSession, labeled_data, user_behavoir_features_table_name, features_dim, job_date)

    my_log("Trainning", level = 1)
    build_lr_model(dataset, 2)
    build_svm_model(dataset, 100)
  }

  def build_lr_model(data_set: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]), classes_num: Int) = {
    val trainSet: RDD[(String, LabeledPoint)] = data_set._1
    val rdd_temp: Array[RDD[(String, LabeledPoint)]] = trainSet.randomSplit(Array(0.8, 0.2))
    val train_rdd: RDD[(String, LabeledPoint)] = rdd_temp(0).cache()
    val valid_rdd: RDD[(String, LabeledPoint)] = rdd_temp(1).cache()

    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(classes_num).run(train_rdd.map(_._2))
    val valid_result = valid_rdd.map(v => (model.predict(v._2.features), v._2.label))

    val (auc, accuracy) = ROC(valid_result)
    printf("\n====>>>> lr_AUC: %.5f, lr_Accuracy: %.5f", auc, accuracy)

    model
  }

  def build_svm_model(data_set: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]), numIterations: Int) = {
    //split the data into training and test sets
    // val dataset_labeled = data_set_1._1
    val dataset_labeled = data_set._1
    val rdd_temp: Array[RDD[(String, LabeledPoint)]] = dataset_labeled.randomSplit(Array(0.8, 0.2))
    val (train_rdd,valid_rdd) = (rdd_temp(0),rdd_temp(1))

    val model = SVMWithSGD.train(train_rdd.map(_._2), numIterations)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = valid_rdd.map(_._2).map(v => (model.predict(v.features), v.label))

    val (auc, accuracy) = ROC(labelAndPreds)
    printf("\n====>>>> SVM_AUC: %.5f, SVM_Accuracy: %.5f", auc, accuracy)

    model
  }


  def get_consumption_label_from_questionnaire(sparkSession: SparkSession, questionnaire_table_name: String) = {
    val select_sql: String = "select imei, q11 from " + questionnaire_table_name + " where Q11!=\"\""
    print_sql(select_sql)

    val consumption_labeled_data = sparkSession.sql(select_sql).filter("imei is not null and q11 is not null").rdd.map(v => (v(0).toString, v(1).toString)).map(v => {
      var label: Double = -1
      if(v._2 == "A")//5000以下
        label = 0
      else //5000以上
        label = 1
      (v._1, label)
    }).filter(_._2 != -1).reduceByKey((a, b) => a)

    numerical_label_distribute(consumption_labeled_data)
    consumption_labeled_data.map(v => (v._1, v._2.toInt))
  }

  def get_data_set_for_build_model(sparkSession: SparkSession,
                                   labeled_dataset: RDD[(String, Int)],
                                   user_behavoir_features_table_name: String,
                                   feature_dim: Int,
                                   yestoday_Date: String): (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]) = {
    // user behavior features
    val select_imei_feature_sql: String = "select * from " + user_behavoir_features_table_name + " where stat_date=" + yestoday_Date
    print_sql(select_imei_feature_sql)

    val imei_feature_rdd: RDD[(String, String)] = sparkSession.sql(select_imei_feature_sql).rdd.map(v => (v(0).toString, v(1).toString))

    // building the train and predict set
    val predict_set_rdd: RDD[(String, LabeledPoint)] = imei_feature_rdd.subtractByKey(labeled_dataset).map(v => {
      val imei: String = v._1
      val feature_str: String = v._2
      val label: Int = -1
      val features: Array[String] = feature_str.split(" ")
      val index_array: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      val value_array: ArrayBuffer[Double] = new ArrayBuffer[Double]()
      for (feature <- features) {
        val columnIndex_value: Array[String] = feature.trim.split(":")
        if (columnIndex_value.length == 2) {
          index_array += columnIndex_value(0).trim.toInt
          value_array += columnIndex_value(1).trim.toDouble
        }
      }
      (imei, label, index_array.toArray, value_array.toArray)
    }).filter(v => v._3.length > 0).map(v => (v._1, new LabeledPoint(v._2, Vectors.sparse(feature_dim, v._3, v._4))))
    printf("\n====>>>> The size of predict set: %d\n", predict_set_rdd.count())

    val train_set_rdd: RDD[(String, LabeledPoint)] = imei_feature_rdd.join(labeled_dataset).map(v => {
      val imei = v._1
      val feature = v._2._1.toString
      val label = v._2._2.toString.toInt
      (imei, feature, label)
    }).map(v => {
      val imei = v._1
      val features: Array[String] = v._2.toString.split(" ")
      val label: Int = v._3
      val index_array: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      val value_array: ArrayBuffer[Double] = new ArrayBuffer[Double]()
      for (feature <- features) {
        val columnIndex_value: Array[String] = feature.trim.split(":")
        if (columnIndex_value.length == 2) {
          index_array += columnIndex_value(0).trim.toInt
          value_array += columnIndex_value(1).trim.toDouble
        }
      }
      (imei, label, index_array.toArray, value_array.toArray)
    }).filter(v => v._3.length > 0).map(v => (v._1, new LabeledPoint(v._2, Vectors.sparse(feature_dim, v._3, v._4))))
    printf("\n====>>>> The size of train set: %d\n", train_set_rdd.count())
    (train_set_rdd, predict_set_rdd)
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

  def my_log(g: String, level: Int) = {
    level match {
      case 1 => printf("\n\n++++++++\n++++++++ %s\n++++++++\n\n", g)
      case 2 => printf("\n\n********  %s  ********\n\n", g)
      case _ => printf("")
    }
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

  def ROC(valid_result: RDD[(Double, Double)]): (Double, Double) = {
    // valid_result (score, label)
    val binary_class_metrics = new BinaryClassificationMetrics(valid_result)

    val roc = binary_class_metrics.roc()
    val au_roc = binary_class_metrics.areaUnderROC()
    val accuracy = valid_result.filter(v => v._1 == v._2).count() * 1.0 / valid_result.count()
    // println("\n\n ********************** AUROC: " + au_roc + " ********************* \n\n")
    return (au_roc, accuracy)
  }
}
