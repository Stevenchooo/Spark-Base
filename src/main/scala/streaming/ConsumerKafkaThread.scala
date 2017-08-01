package sheshou.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.alibaba.fastjson.JSON
import io.netty.handler.codec.string.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

/**
  * Created by fenglu on 2017/4/19.
  */
class ConsumerKafkaThread(topic: String, kafkaParams: Map[String, String], ssc: StreamingContext
                          , sc: SparkContext) extends Runnable {

  def run(): Unit = {

    println("this thread topic is " + topic)

    val sql = "insert into attack_list (attack_time,dst_ip,src_ip,attack_type,src_country_code" +
      ",src_country,src_city,dst_country_code,dst_country,dst_city,src_latitude,src_longitude,dst_latitude" +
      ",dst_longitude,end_time,asset_id,asset_name,alert_level) values (?)"

    var dburl = " "
    var user = " "
    var passwd = " "
    Class.forName("com.mysql.jdbc.Driver")

    var con = DriverManager.getConnection(dburl, user, passwd)
    con.setAutoCommit(false)

    var ps = con.prepareStatement(sql)
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic)).map(_._2)

    var buffer: List[String] = List()
    println("start to process data")
    message.foreachRDD { message =>

      var json = JSON.parseObject(String.valueOf(message))

      var url = json.get("url").toString

      json.put("attacktype", judge(url))

      //      buffer += message.toString
      if (!"no".equals()) {
        //        buffer += json.
      }

      if (buffer.size > 1000) {
        //TODO insetr into mysql
        insert(buffer, con, ps)
      }
    }
  }

  def judge(str: String): String = {

    var rules1 = List("1", "2", "3")
    var rules2 = List("1", "2", "3")

    if (rules1.contains(str)) {
      return "sql"
    }
    else if (rules2.contains(str)) {
      return "xss"
    }
    else if (rules2.contains(str)) {
      return "file"
    }
    else if (rules2.contains(str)) {
      return "command"
    }
    else if (rules2.contains(str)) {
      return "struts"
    }
    else if (rules2.contains(str)) {
      return "path"
    }
    else if (rules2.contains(str)) {
      return "fuzzy"
    }
    else {
      return "no"
    }
  }

  def insert(insert: List[String], con: Connection, ps: PreparedStatement) = {
    println("insert " + insert)
    var i = 0;
    for (i <- 1 to insert.size) {
      ps.setString(1, insert(i).toString)
      ps.addBatch()
    }
    ps.executeBatch()
    con.commit()
  }
}
