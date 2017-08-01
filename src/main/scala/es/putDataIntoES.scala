package com.cloudera.sparkwordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, _}
import org.elasticsearch.spark._

/**
  * Created by steven on 2017/8/1.
  */
object DealES002 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("es test").setMaster("local")
    conf.set("es.net.ssl", "true")
    conf.set("es.net.ssl.keystore.location", "D:\\test_jar\\node-0-keystore.jks")
    conf.set("es.net.ssl.keystore.pass", "changeit")
    conf.set("es.net.ssl.keystore.type", "JKS")
    conf.set("es.net.ssl.truststore.location", "D:\\test_jar\\truststore.jks")
    conf.set("es.net.ssl.truststore.pass", "changeit")
    conf.set("es.net.ssl.cert.allow.self.signed", "false")
    conf.set("es.net.ssl.protocol", "TLSv1.2") //官网上是TLS,必须带版本号，否则报错
    conf.set("es.index.auto.create", "true")
    conf.set("es.net.http.auth.user", "admin") //访问es的用户名
    conf.set("es.net.http.auth.pass", "admin") //访问es的密码
    conf.set("es.nodes.wan.only", "true")
    conf.set("es.index.read.missing.as.empty", "true")
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")

    val sc = new SparkContext(conf)
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")
    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

  }
}
