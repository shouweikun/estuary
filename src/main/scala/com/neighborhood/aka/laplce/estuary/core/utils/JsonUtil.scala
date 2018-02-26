package com.neighborhood.aka.laplce.estuary.core.utils

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * Created by john_liu on 2018/2/26.
  */
object JsonUtil {
  def Json2JavaMap(json:String): util.Map[String, String] = {
   val jsonObject =  JSON.parseObject(json)
   val field =  jsonObject.getClass.getDeclaredField("map")
    field.setAccessible(true)
    field.get(jsonObject).asInstanceOf[java.util.Map[String,String]]
  }
}
