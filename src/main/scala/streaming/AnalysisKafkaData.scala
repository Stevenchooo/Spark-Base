package sheshou.streaming

import java.util.Calendar

import org.apache.commons.codec.StringDecoder
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

/**
  * Created by steven on 2017/5/18.
  */
object AnalysisKafkaData {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""se
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args
    println(brokers)
    println(topics)


    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("AnalysisKafkaData").setMaster("local[*]")
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //spark.hadoop.parquet.enable.summary-metadata false
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)


    // Get the lines
    messages.foreachRDD { x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      //text.toDF().printSchema()

      //register temp table
      val temptable = text.registerTempTable("windowslogin")

      if (text.count() > 0) {


        /* val result = sqlContext.sql("select * from " +
           "(select collectequp,collecttime,statuscode,count(*) as sum " +
           "from windowslogin group by collectequp, collecttime,statuscode)t " +
           "where t.sum >2")*/
        val result = sqlContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where t.sum > 2")

        result.rdd.foreach(println)
        /*result.foreach{
          x=>
            val tmp = AttackList(x.get(0),x.get(1),x.get(2),x.get(3))
        }*/
        val cal = Calendar.getInstance()
        val date = cal.get(Calendar.DATE)
        val Year = cal.get(Calendar.YEAR)
        val Month1 = cal.get(Calendar.MONTH)
        val Month = Month1 + 1
        val Hour = cal.get(Calendar.HOUR_OF_DAY)

        result.write.mode(SaveMode.Append).save("sheshou/data/parquet/realtime/" + "forcebreak" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")
      }

    }


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
