package advertisement

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object Data_process_1 {
  def main(args: Array[String]): Unit = {
    my_log("Initial spark", level = 1)
    val job_time = args(0)
    // val job_time = "20181201"
    val (sparkSession, job_date) = init_job(job_time)

    my_log("getting adv infos", level = 1)
    val adv_ods_infos = get_advs_info(sparkSession, job_date)

    my_log("getting actions of yesterday", level = 1)
    val yesterday_actions = get_action_yestoday(sparkSession, job_date)

    val historical_actions_offset = -1
    my_log("getting click of user about %d months".format(historical_actions_offset), level = 1)
    val historical_actions = get_historical_actions(sparkSession, historical_actions_offset, job_date)
  }

  def get_action_yestoday(sparkSession: SparkSession, job_date: String) = {
    val yesterday = offset_date(job_date, month_offset = 0, day_offset = -1).substring(0, 8)
    // val select_sql = "select imei, unit_id, location, posttime, oper_type from mz_advert.dwd_dsp_ad_monitor_detail_h where stat_date>=" + offset_date(job_date, month_offset = date_offset, day_offset = 0) + " and imei != '__IMEI__' and oper_type in ('all_imp', 'all_clk')"
    val select_sql = "select imei, unit_id, location, posttime, oper_type from mz_advert.dwd_dsp_ad_monitor_detail_h where stat_date like '" + yesterday + "%' and imei != '__IMEI__' and oper_type in ('all_imp', 'all_clk')"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and unit_id is not null and location is not null and posttime is not null and oper_type is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v(3).toString, v(4).toString)).distinct().map(v => {
      var label = 0
      var week = -1
      var hour = -1
      var time = ""

      if (v._4.length == 19) {
        val t = parse_time(v._4)
        week = t._1
        hour = t._2
        time = v._4.substring(0, 13)
      }

      if (v._5 == "all_clk") {
        label = 1
      }
      (v._1, v._2, v._3, week, hour, time, label)
    }).filter(v => v._4 != -1 && v._5 != -1 && v._6 != "").map(v => ((v._1, v._2, v._3, v._4, v._5, v._6), v._7)).reduceByKey(_+_).map(v => (v._1._1, v._1._2, v._1._3, v._1._4, v._1._5, v._1._6, if (v._2 >= 1) 1 else 0))

    //    val data = sparkSession.sql(select_sql).filter("imei is not null and unit_id is not null and location is not null and posttime is not null and oper_type is not null").rdd.map(v => (v(0).toString, v(1).toString, v(2).toString, v(3).toString, v(4).toString)).distinct().map(v => {
    //      var label = 0
    //      var time = ""
    //      if (v._4.length == 19) {
    //        time = v._4.replace("T", " ").substring(0, 13)
    //      }
    //
    //      if (v._5 == "all_clk") {
    //        label = 1
    //      }
    //
    //      (v._1, v._2, v._3, time, label)
    //    }).filter(_._4 != "").map(v => ((v._1, v._2, v._3, v._4), v._5)).reduceByKey(_+_).map(v => (v._1._1, v._1._2, v._1._3, v._1._4, if (v._2 >= 1) 1 else 0)).map(v => {
    //      var week = -1
    //      var hour = -1
    //
    //      val t = parse_time(v._4)
    //      week = t._1
    //      hour = t._2
    //      (v._1, v._2, v._3, week, hour)
    //    })

    printf("\n====>>>> actions of yesterday %s: %d\n", yesterday, data.count())
    numerical_label_distribute(data.map(v => (v._1, v._7)))
    printf("\n====>>>> contained imeis: %d", data.map(_._1).distinct().count())

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_advertisement_user_actions_daily"
    val cols = "imei string, unit_id string, location string, week string, hour string, time string, label int"
    save_result_to_hive(sparkSession, data.toDF(), cols, save_table_name, job_date)
  }

  def get_user_infos(sparkSession: SparkSession) = {
    val select_sql = "select md5(cast(imei as string)), is_mz, sex, user_age, user_educational_status, user_estate, user_owners, phone_operator, phone_model from user_profile.idl_fdt_dw_tag"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null").rdd.map(v => (v(0).toString, (v(1).toString, v(2).toString, v(3).toString, v(4).toString, v(5).toString, v(6).toString, v(7).toString, v(8).toString)))
    printf("\n====>>>> user infos including mz and non-mz: %d\n", data.count())

    data
  }

  def get_advs_info(sparkSession: SparkSession, job_date: String) = {
    val select_sql = "select unit_id, idea_slots, idea_type, adview_type, idea_status from mz_advert.ads_rpt_dsp_adlib_ideas_h"
    // val select_sql = "select idea_slots from mz_advert.ads_rpt_dsp_adlib_ideas_h"

    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("unit_id is not null and idea_slots is not null and idea_type is not null and adview_type is not null and idea_status is not null").rdd.map(v => (v(0).toString, (v(1).toString, v(2).toString, v(3).toString, v(4).toString)))
    printf("\n====>>>> the number of ods(advs): %d\n", data.count())

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_advertisement_adv_features"
    val cols = "unit_id string, idea_slots string, idea_type string, adview_type string, idea_status string"
    save_result_to_hive(sparkSession, data.map(v => (v._1, v._2._1, v._2._2, v._2._3, v._2._4)).toDF(), cols, save_table_name, job_date)

    data
  }

  def get_historical_actions(sparkSession: SparkSession, date_offset: Int, job_date: String) = {
    val select_sql = "select imei, unit_id from mz_advert.dwd_dsp_ad_monitor_detail_h where stat_date>=" + offset_date(job_date, month_offset = date_offset, day_offset = 0) + " and stat_date<" + offset_date(job_date, month_offset = 0, day_offset = -1)+ " and imei != '__IMEI__' and oper_type='all_clk'"
    print_sql(select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and unit_id is not null").rdd.distinct().map(v => (v(0).toString, Array(v(1).toString))).reduceByKey(_++_).map(v => (v._1, v._2.mkString(" ")))
    printf("\n====>>>> the user actions in %d month(s): %d", date_offset, data.count())

    import sparkSession.implicits._
    val save_table_name = "algo.up_yf_advertisement_user_history_click"
    val cols = "imei string, history_click_1m string"
    save_result_to_hive(sparkSession, data.toDF(), cols, save_table_name, job_date)

    data
  }

  def parse_time(t: String) = {
    val time = DateTime.parse(t.replace("T", " "), DateTimeFormat.forPattern("yyyyy-MM-dd HH:mm:ss"))
    val week = time.getDayOfWeek
    val hour = time.getHourOfDay
    (week, hour)
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

  def offset_date(date: String, month_offset: Int, day_offset: Int) = {
    val year: Int = date.substring(0, 4).trim.toInt
    val month: Int = date.substring(4, 6).trim.toInt
    val day: Int = date.substring(6, 8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year, month - 1 + month_offset, day + day_offset)
    val d = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime) + "00"
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
