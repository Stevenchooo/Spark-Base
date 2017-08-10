package http

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

/**
  * Created by steven on 2017/7/10.
  */
object TestHttp {

  def main(args: Array[String]): Unit = {

    val url = "http://192.168.1.21:9999/search/?geoip=192.168.1.1"
    val esurl = "http://localhost:9200/spark/_search?q=osname:windows 7&size=1"

    val urlcons = new URL(esurl)

    val urlcon = urlcons.openConnection()
    val userinfo = "c2hlc2hvdTpzaGVzaG91MTIzNDU="
    urlcon.setRequestProperty("Authorization","Basic " + userinfo)
    urlcon.setRequestProperty("charset", "UTF-8")
    urlcon.setDoOutput(true)
    urlcon.setDoInput(true)

    urlcon.connect()

    val inputvalues = new BufferedReader(new InputStreamReader(urlcon.getInputStream))
    var line = inputvalues.readLine()
    inputvalues.close()
    println(line)

  }
}
