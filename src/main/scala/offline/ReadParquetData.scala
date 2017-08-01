package sheshou.offline

import java.util.Calendar
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by steven on 2017/7/17.
  */
object ReadParquetData {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <src_path> is a list of one or more Kafka brokers
           |  <dest_path> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }


    val Array(windowsloginpath, outputpath) = args
    println(windowsloginpath)
    println(outputpath)

    // val windowsloginpath = "hdfs://192.168.1.21:8020/sheshou/data/parquet/windowslogin/2017/4/16/14"

    val conf = new SparkConf().setAppName("Offline Doc Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //read json file
    val file = sqlContext.read.parquet(windowsloginpath) //.toDF()
    // file.printSchema()
    // println(file.count())

    val temptable = file.registerTempTable("windowslogin") //where loginresult = 537 or loginresult = 529

    //val result = sqlContext.sql("select * from (select  collecttime,destip,srcip,destequp,loginresult,destcountrycode,srccountrycode,srccountry,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,count(*) as sum from windowslogin group by loginresult, destcountrycode,srccountrycode, collecttime,destip,srcip,destequp,srccountry,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude)t where t.sum > 2 and (t.loginresult = 528 or t.loginresult = 529)")

    val tmp = sqlContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where t.sum > 2")

    tmp.printSchema()

    // println(file.count())
    // println(tmp.count())

    tmp.rdd.foreach(println)

    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)
    val Month1 = cal.get(Calendar.MONTH)
    val Month = Month1 + 1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

  }
}
