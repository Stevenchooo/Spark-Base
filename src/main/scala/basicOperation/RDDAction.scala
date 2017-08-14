package basicOperation

import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by cWX491729 on 2017/8/14.
  */
object RDDAction {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("testRDDAction").setMaster("local"))

    /*

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

    /* count操作 */
    println("======count操作======")
    println(rddInt.count())
    println("======count操作======")

    /* countByValue操作 */
    println("======countByValue操作======")
    println(rddInt.countByValue())
    println("======countByValue操作======")

    /* reduce操作 */
    println("======countByValue操作======")
    println(rddInt.reduce((x, y) => x + y))
    println("======countByValue操作======")

    /* fold操作 */
    println("======fold操作======")
    println(rddInt.fold(0)((x, y) => x + y))
    println("======fold操作======")

    /* aggregate操作 */
    println("======aggregate操作======")
    val res: (Int, Int) = rddInt.aggregate((0, 0))((x, y) => (x._1 + x._2, y), (x, y) => (x._1 + x._2, y._1 + y._2))
    println(res._1 + "," + res._2)
    println("======aggregate操作======")

    /* foeach操作 */
    println("======foeach操作======")
    println(rddStr.foreach { x => println(x) })
    println("======foeach操作======")
  }
}
