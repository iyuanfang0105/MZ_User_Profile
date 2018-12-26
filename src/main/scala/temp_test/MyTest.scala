package temp_test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MyTest {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark")
    // val job_time = args(0)
    val job_time = "20180911"
    val (sparkSession, job_date) = init_job(job_time)

    val select_uip = "SELECT imei, user_estate from user_profile.idl_fdt_dw_tag where is_mz=1"
    val select_uip_30 = "SELECT imei, user_estate from user_profile.idl_fdt_dw_tag where is_mz=1 and boot_user_cycle like '%,3,%'"

    print_sql(select_uip)
    print_sql(select_uip_30)

    val uip_data = sparkSession.sql(select_uip).filter("imei is not null and user_estate is not null").rdd.map(v => (v(0).toString, v(1).toString.toInt))
    val uip_data_30 = sparkSession.sql(select_uip_30).filter("imei is not null and user_estate is not null").rdd.map(v => (v(0).toString, v(1).toString.toInt))
    val uip_imei_unknown = uip_data.filter(_._2 == 2)
    val uip_30_imei_unknown = uip_data_30.filter(_._2 == 2)


    val select_raw = "select imei from app_center.adl_fdt_app_adv_model_install where stat_date=20180911"
    val raw_data = sparkSession.sql(select_raw).filter("imei is not null").rdd.map(v => (v(0).toString, 1))


    printf("\n====>>>> uip estate imei: %d\n", uip_data.count())
    printf("\n====>>>> uip estate unknown imei: %d\n", uip_imei_unknown.count())

    printf("\n====>>>> uip_30 estate imei: %d\n", uip_data_30.count())
    printf("\n====>>>> uip_30 estate unknown imei: %d\n", uip_30_imei_unknown.count())

    printf("\n====>>>> algo input imei: %d\n", raw_data.count())

    printf("\n====>>>> join (uip estate unknown imei, algo input imei): %d\n", uip_imei_unknown.join(raw_data).count())
    printf("\n====>>>> join (uip_30 estate unknown imei, algo input imei): %d\n", uip_30_imei_unknown.join(raw_data).count())
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
