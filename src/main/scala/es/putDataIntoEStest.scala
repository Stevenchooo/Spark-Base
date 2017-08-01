package com.cloudera.sparkwordcount

import org.apache.spark.{SparkConf, _}
import org.elasticsearch.spark._

/**
  * Created by steven on 2017/7/31.
  */
object DealES001 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("itTest").setMaster("local")

    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    //将Map对象写入ES
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")
    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")


    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)
    val pairRDD = sc.makeRDD(Seq((3, doc1), (4, doc2)))
    pairRDD.saveToEs("data/test")
  }


}
