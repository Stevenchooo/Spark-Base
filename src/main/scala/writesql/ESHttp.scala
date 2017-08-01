package sheshou.writesql

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

import com.sun.deploy.net.URLEncoder

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by steven on 2017/7/10.
  */
object ESHttp {

  val log = Logger.getLogger("ESHttp")
  var base64auth = "c2hlc2hvdTpzaGVzaG91MTIzNDU="
  var esurlprefix = "http://42.123.99.38:9200/sheshou_info/_search?q=product_type:"
  var esurlmiddle = "AND product_type:"
  var esurlpost = "&size=1"

  /**
    * must be intialed before using this service
    */
  //  def intial(): Unit ={
  //    this.base64auth = SheShouConf.getString(Constants.BASE64AUTH)
  //    this.esurlprefix = SheShouConf.getString(Constants.ESURLPREFIX)
  //    this.esurlmiddle = SheShouConf.getString(Constants.ESURLMIDDLE)
  //    this.esurlpost = SheShouConf.getString(Constants.ESURLPOST)
  //    log.info("intial eshttp service --------------------------------")
  //    log.info("base64auth is "+base64auth)
  //    log.info("esurlprefix is "+esurlprefix)
  //    log.info("esurlpost is "+esurlpost)
  //
  //  }

  /**
    * according the inpur osname query es and return title,vul_type,score_level in map type
    *
    * @param osname
    * @return Map[String,String]
    */
  def getESInfo(osname: String, osversion: String): Map[String, String] = {

    var title = ""
    var vul_type = ""
    var score_level = ""
    try {
      val query = new URL(esurlprefix + URLEncoder.encode(osname + esurlmiddle + osversion, "UTF-8") + esurlpost)
      log.info("query es url is " + query.toString)
      val urlcon = query.openConnection()
      //set Authorization header
      urlcon.setRequestProperty("Authorization", "Basic " + base64auth)
      //set charset UTF-8
      urlcon.setRequestProperty("charset", "UTF-8")
      //set get content from the connect
      urlcon.setDoOutput(true)
      urlcon.setDoInput(true)
      urlcon.connect()
      val esvalues = new BufferedReader(new InputStreamReader(urlcon.getInputStream, "UTF-8"))
      //just take one valid data
      val line = esvalues.readLine()
      esvalues.close()

      //get jsonarray hits[0]
      val array = JSON.parseObject(line).getJSONObject("hits").getJSONArray("hits").get(0)

      //get jsonobject _source
      val values = JSON.parseObject(array.toString).getJSONObject("_source")

      //get detail value
      title = values.get("title").toString
      vul_type = values.get("vul_type").toString
      score_level = values.get("score_level").toString
    }
    catch {
      case ex: Exception => log.error("query fail and detail info is " + ex.getMessage + " stacktrace is " + ex.getStackTrace)
        return Map("title" -> null, "vul_type" -> null, "score_level" -> null)
    }

    return Map("title" -> title, "vul_type" -> vul_type, "score_level" -> score_level)
  }

  /**
    * function test
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //    ESHttp.intial()
    val result = ESHttp.getESInfo("windows 7", "1")
    println(result)
  }
}
