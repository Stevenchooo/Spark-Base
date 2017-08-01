package sheshou.streaming

import java.io.{File, FileInputStream}
import java.util.{Calendar, Properties}
import org.apache.spark.sql.SQLContext
import org.apache.spark._
import org.apache.spark.streaming._


/**
  * Created by steven on 2017/7/17.
  */
object DetectUnabnormal {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    val path = new File("D:\\steven\\conf\\detail.properties")
    properties.load(new FileInputStream(path))
    println(properties.get("master"))

    //Get Date
    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)
    val Month1 = cal.get(Calendar.MONTH)
    val Month = Month1 + 1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

    val appname = String.valueOf(properties.get("appname"))
    val runmodel = String.valueOf(properties.get("runmodel"))
    val zks = String.valueOf(properties.get("zks"))
    val brokers = String.valueOf(properties.get("brokers"))
    //    val timewindow =String.valueOf(properties.get("timewindow"))

    val sparkConf = new SparkConf().setAppName(appname).setMaster(runmodel)
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //spark.hadoop.parquet.enable.summary-metadata false
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    // Create direct kafka stream with brokers and topics
    // val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "zookeeper.connect" -> zks)

    val topics = properties.get("topics").toString

    val topiclist = topics.split(",", 0)

    for (i <- 0 until topiclist.length) {
      val topic = topiclist(i)
      val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic)).map(_._2)
      message.foreachRDD { x =>

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        // val text = sqlContext.read.json(x)
        val text = sqlContext.read.json(x)

        //get json schame
        text.toDF().printSchema()

        //save text into parquet file
        //make sure the RDD is not empty
        if (text.count() > 0) {

          println("write")
          //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")

          text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/" + "webmiddle" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")
        }

      }

    }

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("webmiddle")).map(_._2)

    val messages2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("netstds")).map(_._2)

    val messages3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("windowslogin")).map(_._2)


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
        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")

        text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/" + "webmiddle" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")
      }

    }

    // Get the lines
    messages2.foreachRDD { x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        println("write2")
        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")

        text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/" + "netstds" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")
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

        text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/" + "windowslogin" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")
      }

    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
