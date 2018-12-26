package push

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Push_infer {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181013"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Getting data and features of push")
    val push_features_table = "algo.yf_user_behavior_features_app_install_on_30000_dims_push"
    val push_data_unlabeled: RDD[(String, LabeledPoint)] = get_dataset_push(sparkSession, push_features_table, feature_dim = 30000, job_date)

    my_log("Infer gender")
    infer_gender(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer age")
    infer_age(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer marriage")
    infer_marriage(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer parent")
    infer_parent(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer child")
    infer_child(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer career")
    infer_career(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer education")
    infer_education(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer estate")
    infer_estate(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer car")
    infer_car(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer income")
    infer_income(sparkSession, push_data_unlabeled, job_date)

    my_log("Infer consumption")
    infer_consumption(sparkSession, push_data_unlabeled, job_date)
  }

  def infer_consumption(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/user/wangyao/Base_tag/consumption"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, consumption int"
    val save_table = "algo.up_yf_consumption_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_income(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/user/wangyao/Base_tag/income"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, income int"
    val save_table = "algo.up_yf_income_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_car(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/user/wangyao/Base_tag/carowner"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, car int"
    val save_table = "algo.up_yf_car_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_estate(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/user/wangyao/Base_tag/estateowner"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, estate int"
    val save_table = "algo.up_yf_estate_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_education(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/user/wangyao/Base_tag/education"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, education int"
    val save_table = "algo.up_yf_education_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_career(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/apps/recommend/models/wind/career"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, career int"
    val save_table = "algo.up_yf_career_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_child(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/apps/recommend/models/wind/child"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, child int"
    val save_table = "algo.up_yf_child_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_parent(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/apps/recommend/models/wind/parent"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, parent int"
    val save_table = "algo.up_yf_parent_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_marriage(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/apps/recommend/models/wind/marriage"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, marriage int"
    val save_table = "algo.up_yf_marriage_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_age(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/apps/recommend/models/wind/age"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, age int"
    val save_table = "algo.up_yf_age_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer_gender(sparkSession: SparkSession, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model_path = "/apps/recommend/models/wind/gender"
    print_sql(model_path)
    val pred = infer(sparkSession, model_path, data_unlabeled, job_date)

    import sparkSession.implicits._
    val cols = "imei string, gender int"
    val save_table = "algo.up_yf_gender_push"
    save_result_to_hive(sparkSession, pred.toDF(), cols, save_table, job_date)
  }

  def infer(sparkSession: SparkSession, model_path: String, data_unlabeled: RDD[(String, LabeledPoint)], job_date: String) = {
    val model = load_model(sparkSession, model_path)
    val pred = data_unlabeled.map(v => (v._1, model.predict(v._2.features))).distinct()
    numerical_label_distribute(pred)
    pred
  }

  def load_model(sparkSession: SparkSession, model_path: String): LogisticRegressionModel = {
    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val p = new Path(model_path)
    var model: LogisticRegressionModel = null
    if (fs.exists(p)) {
      printf("\n====>>>> load model from: %s", p.toString)
      model = LogisticRegressionModel.load(sparkSession.sparkContext, p.toString)
    }
    model
  }

  def get_dataset_push(sparkSession: SparkSession, imei_features_table: String, feature_dim: Int, job_date: String): RDD[(String, LabeledPoint)] = {

    val select_features_sql: String = "select imei, feature from " + imei_features_table + " where stat_date=" + job_date
    printf("\n====>>>> %s\n", select_features_sql)
    val imei_features_df = sparkSession.sql(select_features_sql).filter("imei is not null and feature is not null")

    if (imei_features_df.count() == 0){
      printf("\n====>>>> no data of %s\n", job_date)
      sparkSession.sparkContext.emptyRDD[(String, LabeledPoint)]
    }else {
      val dataset: RDD[(String, String, Double)] = imei_features_df.rdd.map(v => (v(0).toString, v(1).toString, -1))

      val dataset_label_point = dataset.map(v => {
        val imei = v._1
        val features_array = v._2.split(" ")
        val index_array = new ArrayBuffer[Int]()
        val value_array = new ArrayBuffer[Double]()

        for (item <- features_array){
          val index_value = item.trim.split(":")
          if (index_value.length == 2){
            index_array += index_value(0).toInt
            value_array += index_value(1).toDouble
          }
        }
        (imei, index_array, value_array, v._3)
      }).filter(v => v._2.nonEmpty && v._3.nonEmpty).map(v => (v._1, new LabeledPoint(v._4, Vectors.sparse(feature_dim, v._2.toArray, v._3.toArray))))

      numerical_label_distribute(dataset_label_point.map(v => (v._1, v._2.label)))

      val dataset_labeled = dataset_label_point.filter(_._2.label != -1)
      val dataset_unlabeled = dataset_label_point.filter(_._2.label == -1)
      printf("\n====>>>> dataset_labeled: %d\n", dataset_labeled.count())
      printf("\n====>>>> dataset_unlabeled: %d\n", dataset_unlabeled.count())

      dataset_unlabeled
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
