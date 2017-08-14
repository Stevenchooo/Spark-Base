package com.cloudera.sparkwordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by steven on 2017/7/31.
  */
object testRDD {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Steven005").setMaster("local"))

    /*
       map()           参数是函数，函数应用于RDD每一个元素，返回值是新的RDD
      flatMap()       参数是函数，函数应用于RDD每一个元素，将元素数据进行拆分，变成迭代器，返回值是新的RDD
      filter()        参数是函数，函数会过滤掉不符合条件的元素，返回值是新的RDD
      distinct()      没有参数，将RDD里的元素进行去重操作
      union()         参数是RDD，生成包含两个RDD所有元素的新RDD
      intersection()  参数是RDD，求出两个RDD的共同元素
      subtract()      参数是RDD，将原RDD里和参数RDD里相同的元素去掉
      cartesian()     参数是RDD，求两个RDD的笛卡儿积

      collect()                     返回RDD所有元素
      count()                       RDD里元素个数
      countByValue()                各元素在RDD中出现次数
      reduce()                      并行整合所有RDD数据，例如求和操作
      fold(0)(func)                 和reduce功能一样，不过fold带有初始值
      aggregate(0)(seqOp,combop)    和reduce功能一样，但是返回的RDD数据类型和原RDD不一样
      foreach(func)                 对RDD每个元素都是使用特定函数

    */
    val rddInt: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 2, 5, 1))
    val rddStr: RDD[String] = sc.parallelize(Array("a", "b", "c", "d", "b", "a"), 1)
    val rddFile: RDD[String] = sc.textFile("1.txt", 1)

    val rdd01: RDD[Int] = sc.makeRDD(List(1, 3, 5, 3))
    val rdd02: RDD[Int] = sc.makeRDD(List(2, 4, 5, 1))

    /* map操作 */
    println("======map操作======")
    println(rddInt.map(x => x + 1).collect().mkString(","))
    println("======map操作======")

    /* filter操作 */
    println("======filter操作======")
    println(rddInt.filter(x => x > 4).collect().mkString(","))
    println("======filter操作======")

    /* flatMap操作 */
    println("======flatMap操作======")
    println(rddFile.flatMap { x => x.split(",") }.first())
    println("======flatMap操作======")

    /* distinct去重操作 */
    println("======distinct去重======")
    println(rddInt.distinct().collect().mkString(","))
    println(rddStr.distinct().collect().mkString(","))
    println("======distinct去重======")

    /* union操作 */
    println("======union操作======")
    println(rdd01.union(rdd02).collect().mkString(","))
    println("======union操作======")

    /* intersection操作 */
    println("======intersection操作======")
    println(rdd01.intersection(rdd02).collect().mkString(","))
    println("======intersection操作======")

    /* subtract操作 */
    println("======subtract操作======")
    println(rdd01.subtract(rdd02).collect().mkString(","))
    println("======subtract操作======")

    /* cartesian操作 */
    println("======cartesian操作======")
    println(rdd01.cartesian(rdd02).collect().mkString(","))
    println("======cartesian操作======")
  }

}
