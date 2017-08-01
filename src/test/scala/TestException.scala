import java.util.Properties

/**
  * Created by steven on 2017/7/6.
  */
object TestException {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("sheshou").setMaster("local[2]")
    val sparkcontext = new SparkContext(sparkconf)
    val sqlcontext = new SQLContext(sparkcontext)

    val dburl = "jdbc:mysql://192.168.1.22:3306/steven"
    val user = "root"
    val password = "root"
    val tablename = "testexceptionconnect"

    val props = new Properties()
    props.setProperty("user",user)
    props.setProperty("password",password)

    val datardd = sparkcontext.makeRDD(List("{\"ip\":\"52.222.174.250\"}"))
    val badipset = sparkcontext.textFile("C:\\Users\\Desktop\\extendIP2").collect().toSet

    val checkedrdd = datardd.filter{
      data => badipset.contains(getIp(data))
    }

    checkedrdd.foreach(println(_))

    sqlcontext.read.json(checkedrdd).write.mode(SaveMode.Append).option("driver", "com.mysql.jdbc.Driver").jdbc(dburl,tablename,props)

  }

  def getIp(data:String): String ={
    return JSON.parseObject(data).get("ip").toString

  }
}
