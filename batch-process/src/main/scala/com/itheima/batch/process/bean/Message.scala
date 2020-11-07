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
    val messageTotal: TotalMessage = JSON.parseObject(json, classOf[TotalMessage])
    val message: String = messageTotal.message
    val mapValue: JSONObject = JSON.parseObject(message, classOf[JSONObject])
    Message(
      mapValue.getOrDefault("ad_campaign","").toString,
      mapValue.getOrDefault("ad_media","").toString,
      mapValue.getOrDefault("ad_source","").toString,
      mapValue.getOrDefault("city","").toString,
      mapValue.getOrDefault("corpurin","").toString,
      mapValue.getOrDefault("device_type","").toString,
      mapValue.getOrDefault("host","").toString,
      mapValue.getOrDefault("t_id","0").asInstanceOf[Int],
      mapValue.getOrDefault("timestamp","").toString,
      mapValue.getOrDefault("userId","").toString,
      mapValue.getOrDefault("click_user_id","").toString
    )
  }
}
