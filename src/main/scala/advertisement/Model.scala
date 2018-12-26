package advertisement

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import scala.collection.mutable.ArrayBuffer

object Model {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark", level = 1)
    val job_time = args(0)
    // val job_time = "20181211"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("Getting trianning data", level = 1)
    val train_data_duration = -1
    val (features, data_set) = get_trainning_data(sparkSession, train_data_duration, job_date)

    my_log("Trainning", level = 1)
    val negative_fraction = 1.0
    my_log("negative sample fraction: %f".format(negative_fraction), level = 2)
    val model = trainning(data_set, negative_fraction)
    val weights = model.weights

    val features_weights = new ArrayBuffer[String]()
    for (i <- 0 until weights.size) {
      if (features(i).split("_")(0) != "time") {
        features_weights += (features(i) + " " + weights(i).formatted("%.5f"))
      }
    }


//    import sparkSession.implicits._
//    sparkSession.sparkContext.parallelize(features_weights).repartition(1).map(v => v._1 + " " + v._2.formatted("%.5f")).toDF().write.mode(SaveMode.Overwrite).text("/apps/recommend/models/online/uip_dsp_ctr_lr_v0/" + job_date)
//    sparkSession.sparkContext.parallelize(List(job_date)).repartition(1).toDF().write.mode(SaveMode.Overwrite).text("/apps/recommend/models/online/uip_dsp_ctr_lr_v0/check")
    val model_save_path = "/apps/recommend/models/online/uip_dsp_ctr_lr_v0"
    save_model(features_weights.toArray, model_save_path, job_date)
    save_check(model_save_path, job_date)
  }

  def trainning(data_set: RDD[LabeledPoint], downsample: Double) = {
    val tmp = data_set.randomSplit(Array(0.8, 0.2), seed = 123L)
    var train_ds = tmp(0)
    val test_ds = tmp(1)
    printf("\n====>>>> train_ds: %d test_ds: %d\n", train_ds.count(), test_ds.count())

    if (downsample > 0) {
      val l_0 = train_ds.filter(_.label == 0)
      val l_1 = train_ds.filter(_.label == 1)
      train_ds = l_0.sample(withReplacement = false, fraction = downsample, seed = 123L).union(l_1).sample(withReplacement = false, fraction = 1.0, seed = 123L)
    }
    numerical_label_distribute(train_ds.map(v => ("1", v.label)))

    var model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(train_ds)

    val test_res = test_ds.map(v => (model.predict(v.features), v.label))
    val (auc, accu) = ROC(test_res)
    printf("\n====>>>> AUC: %f, Accuracy: %f\n", auc, accu)

    model.clearThreshold()
    val test_res_1 = test_ds.map(v => (model.predict(v.features), v.label))
    val metrics = new BinaryClassificationMetrics(test_res_1)
    val fMeasure = metrics.fMeasureByThreshold()
    val best_threshold = fMeasure.reduce((a, b) => {
      if (a._2 < b._2) b else a
    })._1

    model.setThreshold(best_threshold)
    val test_res_2 = test_ds.map(v => (model.predict(v.features), v.label))
    val (auc_2, accu_2) = ROC(test_res_2)
    printf("\n====>>>> Select best threshold: %f, AUC: %f, Accuracy: %f\n", best_threshold, auc_2, accu_2)

    model
  }

  def get_trainning_data(sparkSession: SparkSession, duration: Int, job_date: String) = {
    val select_sql = "select imei, unit_id as unitId, location, week, hour, time, is_mz as isMz, sex, user_age as userAge, user_educational_status as userEducationalStatus, user_estate as userEstate, user_owners as userOwners, phone_operator as phoneOperator, phone_model as phoneModel, idea_slots as ideaSlots, idea_type as ideaType, adview_type as adviewType, idea_status as ideaStatus, history_click_1m as hitoryClick1m, label, stat_date from algo.up_yf_advertisement_trainning_data_daily where stat_date>=" + offset_date(job_date, month_offset = duration, day_offset = 0)
    print_sql(select_sql)

    val df = sparkSession.sql(select_sql).repartition(100)
    val features_name = df.columns

    val data = df.rdd.map(v => {
      val s = v.toSeq.map(x => if (x == null) "" else x.toString)
      val feats = (for (i <- 1 until features_name.length - 2) yield {
        (features_name(i), s(i).trim.split(" "))
      }).toMap.filter(_._2.nonEmpty)
      val label = s(features_name.length - 2).toDouble
      val feats_str = feats.toArray.flatMap(v => for (i <- 0 until v._2.length) yield v._1 + "_" + v._2(i).replaceAll("_", "")).mkString(" ")
      (feats_str, label)
    }).reduceByKey(_ + _).map(v => (v._1.split(" "), if (v._2 >= 1) 1 else 0))

    import sparkSession.implicits._
    data.map(v => (v._1.mkString(" "), v._2)).repartition(10).map(v => v._1 + "##yf##" + v._2.toString).toDF().write.mode(SaveMode.Overwrite).text("/apps/recommend/models/wind/uip_adv/labelled_ds/")

    numerical_label_distribute(data.map(v => ("1", v._2)))

    val features_map = data.flatMap(_._1).distinct().sortBy(v => v).zipWithIndex().collectAsMap()
    val features_dim = features_map.size

    val data_set = data.map(v => {
      val feat_arr = v._1
      val index_arr = new ArrayBuffer[Int]()
      val value_arr = new ArrayBuffer[Double]()

      for (i <- feat_arr.indices) {
        if (features_map.contains(feat_arr(i))) {
          index_arr += features_map(feat_arr(i)).toInt
          value_arr += 1.0
        }
      }
      // (v._2, Vectors.sparse(features_dim, index_arr.toArray.sortBy(v => v), value_arr.toArray))
      new LabeledPoint(v._2, Vectors.sparse(features_dim, index_arr.toArray.sortBy(v => v), value_arr.toArray))
    })

    (features_map.map(v => (v._2, v._1)), data_set)
  }

  def ROC(valid_result: RDD[(Double, Double)]): (Double, Double) = {
    val binary_class_metrics = new BinaryClassificationMetrics(valid_result)

    val roc = binary_class_metrics.roc()
    val au_roc = binary_class_metrics.areaUnderROC()
    val accuracy = valid_result.filter(v => v._1 == v._2).count() * 1.0 / valid_result.count()
    // println("\n\n ********************** AUROC: " + au_roc + " ********************* \n\n")
    return (au_roc, accuracy)
  }

  def save_model(data: Array[String], hdfs_path: String, job_date: String) = {
    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val p = new Path(hdfs_path + "/" + job_date + "/uip_dsp_lr_model_weights_v0.txt")
    if (fs.exists(p)) {
      printf("\n====>>>> model file is alread exists! delete it! %s", p.toString)
      fs.delete(p, true)
    }

    printf("\n====>>>> model save to: %s\n", p.toString)
    val os: FSDataOutputStream = fs.create(p)
    data.foreach(v => os.write((v + "\n").getBytes()))
    os.flush()
    os.close()
  }

  def save_check(hdfs_path: String, job_date: String) = {
    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val p = new Path(hdfs_path + "/check")
    if (fs.exists(p)) {
      printf("\n====>>>> check file is alread exists! delete it! %s", p.toString)
      fs.delete(p, true)
    }

    printf("\n====>>>> check file save to: %s\n", p.toString)
    val os: FSDataOutputStream = fs.create(p)
    os.write(job_date.getBytes())
    os.flush()
    os.close()
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
      case 2 => printf("\n\n======  %s  ======\n\n", g)
      case _ => printf("")
    }
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
    labeled_data.map(v => (v._2, 1)).reduceByKey(_ + _).collect().sortBy(_._2).foreach(f = v => {
      printf("\n====>>>> label_%s: %d, all: %d, ratio: %.4f\n", v._1, v._2, labeled_data_count, v._2 * 1.0 / labeled_data_count)
    })
  }
}
