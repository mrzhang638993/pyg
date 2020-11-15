package com.itheima.realprocess.bean

import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * 新建的样例类对象
 * */
case class AdClickLog(
                       city:String,
                       ad_compaign:String,
                       ad_media:String,
                       ad_source:String,
                       corpurin:String,
                       device_type:String,
                       host:String,
                       t_id:String,
                       user_id:String,
                       click_user_id:String,
                       timestamp:String
                     )


object AdClickLog{
  def apply(jsonStr:String): AdClickLog = {
    //解析字符串得到对应的对象
    val jsonObject: JSONObject = JSON.parseObject(jsonStr)
    val city: String = jsonObject.getString("city")
    val ad_compaign: String = jsonObject.getString("ad_compaign")
    val ad_media: String = jsonObject.getString("ad_media")
    val ad_source: String = jsonObject.getString("ad_source")
    val corpurin: String = jsonObject.getString("corpurin")
    val device_type: String = jsonObject.getString("device_type")
    val host: String = jsonObject.getString("host")
    val t_id: String = jsonObject.getString("t_id")
    val user_id: String = jsonObject.getString("user_id")
    val click_user_id: String = jsonObject.getString("click_user_id")
    val timestamp: String = jsonObject.getString("timestamp")
    new AdClickLog(city,ad_compaign,ad_media,ad_source,corpurin,device_type,host,t_id,user_id,click_user_id,timestamp)
  }
}
