package sheshou.streaming

import java.sql.{Connection, DriverManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

/**
  * Created by steven on 2017/7/17.
  */
class GetMysqlConnection (dburl:String, user: String, password:String){

  def getCon(): Connection ={
    Class.forName("com.mysql.jdbc.Driver")
    val con = DriverManager.getConnection(dburl,user,password)
    return con
  }
}
