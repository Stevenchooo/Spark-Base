package sheshou.predict


import java.sql.{DriverManager, ResultSet}

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
/**
  * Created by steven on 2017/7/19.
  */
object DailyPrediction {

  case class HourPredict(id:Int,busiess_sys:String,time_hour:String,attack_type:String,real_count:Int,predict_count:Int)

  case class MidData(hour:String, vulnerability:Int,predict:Int)
  case class HourStatus(hour:String, vulnerability:Int)
  //define method
  def compareInputs(input: Array[HourStatus]): MidData = {
    var result:HourPredict =HourPredict(0,"","","",0,0)

    //初始化变量
    var id = 0
    var business = ""
    var hour = ""
    var attack = ""
    var current = 0
    var next = 0
    //increase percentage
    var increase:Double = 0

    var current_time = ""
    //打印变量长度
    println("****"+input.length)

    if(input.length >= 2){
      val st = input.takeRight(2)

      //有效数据
      if((st(1).vulnerability!=0)&&(st(1).vulnerability!= 0)){

        println("st1"+st(1).vulnerability+"st(0)"+st(0).vulnerability)
        //计算增长率
        increase = (st(1).vulnerability-st(0).vulnerability).toDouble/st(0).vulnerability.toDouble
        current_time = st(1).hour
        current = st(1).vulnerability
        //预测下一个
        next = (st(1).vulnerability.toDouble *(1.0+increase)).toInt
      }

    }
    else{

      val st = input.take(1)
      current = st(0).vulnerability
      current_time = st(0).hour
      next = st(0).vulnerability
    }

    return MidData(current_time, current,next)

  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            | <hiveurl> is a list of one or more Kafka brokers
                            |  <databasename> is a list of one or more kafka topics to consume from
                            |  <tablename1>
                            |  <col_name>
                            |  <tablename2>
        """.stripMargin)
      System.exit(1)
    }



    val Array(hiveurl, databasename,tablename1,col_name,tablename2) = args
    println(hiveurl)
    println(databasename)
    println(tablename1)
    println(col_name)
    println(tablename2)


    val conf = new SparkConf().setAppName("Daily Prediction Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //create hive context
    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    val conn = DriverManager.getConnection("jdbc:hive2://"+hiveurl+"/"+databasename+"?hive.execution.engine=mr")

    //get input table
    val source: ResultSet = conn.createStatement
      .executeQuery("SELECT time_day,"+col_name+"  FROM "+tablename1)
    //fetch all the data
    val fetchedSrc = mutable.MutableList[HourStatus]()
    while(source.next()) {
      var rec = HourStatus(
        source.getString("time_day"),
        source.getInt(col_name)
      )
      fetchedSrc += rec
    }

    val predict = compareInputs(fetchedSrc.toArray)

    println("predict: "+ predict.vulnerability)

    val insertSQL = "Insert into table "+ databasename+"."+tablename2+" values ( "+predict.predict+",\"0\",\""+predict.hour+"\",\""+col_name+"\","+predict.vulnerability+","+predict.predict+")"

    println(insertSQL)

    conn.createStatement.execute(insertSQL)

  }
}
