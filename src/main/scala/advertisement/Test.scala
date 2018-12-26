package advertisement

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    val job_time = args(0)
    // val job_time = "20181118"
    val (sparkSession, job_date) = init_job(job_time)
  }

  def get_samples(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select * from algo.acgn_sample_features_v1 where stat_date=20180408"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql)

    // get the name of columns
    val columns = data.columns

    // (feature_map,label)
    val rdd_features_label = data.rdd.map(v => {
      val col = v.toSeq.map(x => if (x == null) "" else x.toString)
      val features_map = (for(i<-0 until columns.length - 2) yield {//label,stat_date不读
        (columns(i), col(i).trim.split(","))
      }).toMap.filter(v => v._2.length > 0)
      val label = col(columns.length - 2).toDouble
      val feature_arr = features_map.toArray.flatMap(v=>for(j<-0 until v._2.length) yield (v._1,v._1+"_"+v._2(j).replaceAll("_","")))
      (label,feature_arr)
    })

    // features selection
    val feature_prefix_arr = Array("i1","i2","i3","i4","i5","i6","i7","a1","a2","a3","a4","a5","a6","a7","a8")
    // features cross
    val feature_cross_arr = Array("i1*g0","i2*g0","i11*g0","i15*g8","i16*g8","i17*g8")

    val sample_feature_info = rdd_features_label.map(v=>{
      val prefix_feature_arr = v._2.filter(v=>feature_prefix_arr.contains(v._1))
      val feature_arr = prefix_feature_arr.map(_._2).distinct
      (v._1,feature_arr)
    })

    //对样本中出现的所有特征建立索引
    val feature_index_map = sample_feature_info.flatMap(v=>v._2).distinct().sortBy(v=>v).zipWithIndex().collectAsMap()
    val dim_num = feature_index_map.size
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
