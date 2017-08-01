package sheshou.streaming

import java.sql.{Connection, DriverManager}

/**
  * Created by steven on 2017/7/17.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

object DataStat {

  def stat(sqlcontext:SQLContext,dataFrame: DataFrame, con: Connection) : Unit = {
    dataFrame.printSchema()
    println(dataFrame.count())
    dataFrame.registerTempTable("statics")

    if(dataFrame.count()>0) {
      var hoursql = "select INT(sum(attack_type)) as count, DATE_FORMAT(concat(year,'-',month,'-',day,' ',hour),'YYYY-MM-dd HH:mm:ss') as time_hour from statics where attack_type = 'mal_operation' group by year,month,day,hour"
      var daysql = "select INT(sum(attack_type)) as count, DATE_FORMAT(concat(year,'-',month,'-',day),'YYYY-MM-dd HH:mm:ss') as time_day from statics where attack_type = 'mal_operation' group by year,month,day"
      var hourinsertsql = "insert into hourly_stat (time_hour, mal_operation) values(?,?) ON DUPLICATE KEY UPDATE mal_operation = mal_operation + ?"
      var dayinsertsql = "insert into dayly_stat (time_day, mal_operation) values(?,?) ON DUPLICATE KEY UPDATE mal_operation = mal_operation + ?"

      var hourstatics = sqlcontext.sql(hoursql)
      var daytatics = sqlcontext.sql(daysql)

      var hourps = con.prepareStatement(hoursql)
      var dayps = con.prepareStatement(daysql)

      var hourinsert = hourstatics.collect()
      var dayinsert = daytatics.collect()

      for (info <- hourinsert) {
        var count = info.get(0)
        var timedate = info.get(1)
        println("count is " + count + " timedate is " + timedate)

        hourps.setObject(1, timedate)
        hourps.setObject(2, count)
        hourps.setObject(3, count)
        hourps.addBatch()

      }
      hourps.executeBatch()
      con.commit()

      for (info <- dayinsert) {
        var count = info.get(0)
        var timedate = info.get(1)
        println("count is " + count + " timedate is " + timedate)

        dayps.setObject(1, timedate)
        dayps.setObject(2, count)
        dayps.setObject(3, count)

      }
      dayps.executeBatch()
      con.commit()

      hourps.close()
      dayps.close()
      con.close()
    }
  }

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("sheshou").setMaster("local[2]")
    val sparkcontext = new SparkContext(sparkconf)

    val dburl = "jdbc:mysql://192.168.1.22:3306/nssa_db"
    val user = "root"
    val password = "root"

    val options = Map("header" -> "true", "path" -> "C:\\Users\\Desktop\\net.csv")
    val sqlcontext = new SQLContext(sparkcontext)
    val text = sqlcontext.read.options(options).format("com.databricks.spark.csv").load()

    Class.forName("com.mysql.jdbc.Driver")
    val con = DriverManager.getConnection(dburl,user,password)
    con.setAutoCommit(false)

    stat(sqlcontext,text,con)

  }
}
