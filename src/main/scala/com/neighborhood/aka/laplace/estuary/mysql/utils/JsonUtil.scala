package com.neighborhood.aka.laplace.estuary.mysql.utils

import java.io._
import java.util

import com.alibaba.fastjson.JSON
import com.fasterxml.jackson.databind.ObjectMapper
import com.neighborhood.aka.laplace.estuary.mysql.utils

/**
  * Created by john_liu on 2018/2/26.
  */
object JsonUtil {


  /**
    * 针对classpath的
    */
  val CLASSPATH_SCHEMA = "classpath:"
  /**
    * 针对文件系统的
    */
  val FILE_SCHEMA = "file:///"

  def Json2JavaMap(json: String): util.Map[String, String] = {
    val jsonObject = JSON.parseObject(json)
    val field = jsonObject.getClass.getDeclaredField("map")
    field.setAccessible(true)
    field.get(jsonObject).asInstanceOf[java.util.Map[String, String]]
  }

  def parse[T](confPath: String, clazz: Class[T]): T = {
???
//    val mapper = new ObjectMapper
//    //        InputStream is = null;
//    val result = try { //            if (confPath.startsWith(CLASSPATH_SCHEMA)) {
//      //                is = ClassLoader.getSystemClassLoader().getResourceAsStream(confPath.substring(CLASSPATH_SCHEMA.length()));
//      //            } else if (confPath.startsWith(FILE_SCHEMA)) {
//      //                is = new FileInputStream(confPath.substring(FILE_SCHEMA.length()));
//      //            } else {
//      //                throw new RuntimeException("confPath:" + confPath + " must be start with" + CLASSPATH_SCHEMA + " or " + FILE_SCHEMA);
//      //            }
//      val content = getFileContent(confPath)
//      mapper.readValue(content, clazz)
//    }
//    result
  }

  def serialize(`object`: Any): String = {
    val mapper = new ObjectMapper
    var json:String = null
    try
      json = mapper.writeValueAsString(`object`)
    catch {
      case e: IOException =>
//        LOGGER.error("使用jackson序列化{}时报错", `object`)
        e.printStackTrace()
    }
    json
  }

  def getFileContent(filePath: String): String = {
    ???
//    var is:InputStream = null
//    val sb = new StringBuilder
//    try {
//      if (filePath.startsWith(CLASSPATH_SCHEMA)) is = ClassLoader.getSystemClassLoader.getResourceAsStream(filePath.substring(CLASSPATH_SCHEMA.length))
//      else if (filePath.startsWith(FILE_SCHEMA)) is = new FileInputStream(filePath.substring(FILE_SCHEMA.length))
//      else throw new RuntimeException("confPath:" + filePath + " must be start with" + CLASSPATH_SCHEMA + " or " + FILE_SCHEMA)
//      var line:String = null
//      val reader = new BufferedReader(new InputStreamReader(is))
//      while ( {
//        (line = reader.readLine) != null
//      }) sb.append(line)
//    } catch {
//      case e: IOException =>
//        throw new RuntimeException(e)
//    } finally if (is != null) try
//      is.close()
//    catch {
//      case e: IOException =>
//        e.printStackTrace()
//    }
//    sb.toString
  }
}
