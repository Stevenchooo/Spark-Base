package sheshou.mysql

import java.sql.Connection

import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by steven on 2017/7/17.
  */
object mysqlUtil {
  val logger = LoggerFactory.getLogger(this.getClass)
  //  case class insertformat(wid:String,username:String,source:String) extends Serializable
  def insertIntoMysql(con: Connection, sql: String, data: RDD[(String, String, String)]) = {
    val ps = con.prepareStatement(sql)
    try {
      // close autocommit
      con.setAutoCommit(false)
      data.foreach(data => {
        ps.setString(1, data._1)
        ps.setString(2, data._2)
        ps.setString(3, data._3)
        ps.addBatch()
      })

      ps.executeBatch()
      con.commit()
    } catch {
      case e: Exception =>
        logger.error("Error in execution of insert. " + e.getMessage)
    }
  }
}


