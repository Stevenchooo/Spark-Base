package com.cloudera.sparkwordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by steven on 2017/7/31.
  */
object Test005 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Steven005").setMaster("local"))

    //-----------------------------------------------------------------------------------
    val list = List(1,2,3,4,5,6);
    val rdd01 = sc.makeRDD(list)
    val r1 = rdd01.map {x => x*x}
    print(r1.collect().mkString(" , "))

    val rdd02 = sc.makeRDD(Array(1,23,4,6,5))
    val r2 = rdd02.map {x => x < 5}
    print(r2.collect().mkString(" , "))


    val rdd03 = sc.parallelize(list, 1 )
    val r03 = rdd03.map { x => x + 1 }
    println(r03.collect().mkString(","))

    val r04 = rdd03.filter { x => x > 3 }
    println(r04.collect().mkString(","))

    //新建Tuple
    val pair = (99, "Luftballons" , sc.getConf.toString)
    println(pair._3)

    val rdd = sc.textFile("1.txt" , 1);
    val r = rdd.flatMap { x => x.split(" ") }
    println(r.collect().mkString(","))
    //-----------------------------------------------------------------------------------
    /*Transformation &
    去重distinct()，合并union()，共同元素intersection()，去掉两个RDD相同的元素subtract()，
    笛卡尔积cartesian()
    */
    /* Action
    返回所有元素collect()，个数count()，各元素出现次数countByValue()，并行整合所有元素reduce()。
    fold(0)(func)，aggregate(0)(seqOp,combop)，对每个元素使用特定函数foreach(func)
    */









  }

}
