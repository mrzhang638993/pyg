package com.itheima.batch.process.bean

import com.alibaba.fastjson.{JSON, JSONObject}

case class Message(
                  var adCampaign:String,
                  var adMedia:String,
                  var adSource:String,
                  var city:String,
                  var corpurin:String,
                  var deviceType:String,
                  var host:String,
                  var tId:Int,
                  var timestamp:String,
                  var userId:String,
                  var clickUserId:String
                  )

object Message{

  def apply(json:String): Message ={
    //  名称不对应的
    println(json)
    val jsonObject: JSONObject = JSON.parseObject(json, classOf[JSONObject])
    val mapValue: AnyRef = jsonObject.get("message")
    if(mapValue!=null){
      println(mapValue)
    }
    /*new Message(
      mapValue.get("ad_campaign").toString,
      mapValue.get("ad_media").toString,
      mapValue.get("ad_source").toString,
      mapValue.get("city").toString,
      mapValue.get("corpurin").toString,
      mapValue.get("device_type").toString,
      mapValue.get("host").toString,
      mapValue.get("t_id").get.toInt,
      mapValue.get("timestamp").toString,
      mapValue.get("userId").toString
    )*/
    null
  }

}