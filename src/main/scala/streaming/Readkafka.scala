package sheshou.streaming


import java.util.{Calendar, Properties}

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by steven on 2017/7/17.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

object Readkafka {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }


    val Array(brokers, url, user, passwd) = args
    println(brokers)
    println(url)
    println(user)
    println(passwd)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("SaveKafkaData")
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //spark.hadoop.parquet.enable.summary-metadata false
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))


    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "zookeeper.connect" -> "localhost:2181")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("webmiddle")).map(_._2)

    //    val messages2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //      ssc, kafkaParams, Set("netstds")).map(_._2)

    val messages3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("windowslogin")).map(_._2)

    val messgesnet = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("netstdsonline")).map(_._2)

    //Get Date
    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)
    val Month1 = cal.get(Calendar.MONTH)
    val Month = Month1 + 1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

    // Get the lines
    messages.foreachRDD { x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        println("write")
        text.write.format("parquet").mode(SaveMode.Append).parquet("/sheshou/data/parquet/" + "webmiddle" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")
      }

    }


    // Get the lines
    messages3.foreachRDD { x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        println("write3")
        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")

        text.write.format("parquet").mode(SaveMode.Append).parquet("/sheshou/data/parquet/" + "windowslogin" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")
      }

    }

    messgesnet.foreachRDD { x =>
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")

        text.write.format("parquet").mode(SaveMode.Append).parquet("/sheshou/data/parquet/realtime/break" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)

        text.registerTempTable("netstds")
        val mysqlDF = sqlContext.sql("select \"0\" as id, attack_time,  dst_ip,src_ip, \"mal_operation\" as attack_type,src_country_code,  src_country, src_city, dst_country_code, dst_country,dst_city,src_latitude,  src_longitude,  dst_latitude, dst_longitude, end_time,  asset_id, asset_name, alert_level  from netstds ")
        mysqlDF.printSchema()
        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

        dfWriter.jdbc(url, "attack_list", prop)
      }
    }
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
