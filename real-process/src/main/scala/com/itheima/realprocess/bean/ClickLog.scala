package com.itheima.realprocess.bean

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}

case class ClickLog(channelID:Long,
                    categoryID:Long,
                    produceID:Long,
                    userID:Long,
                    country:String,
                    province:String,
                    city:String,
                    network:String,
                    source:String,
                    browserType:String,
                    entryTime:Long,
                    leaveTime:Long)

object  ClickLog{
  def apply(json:String): ClickLog ={
    val jsonObject: JSONObject = JSON.parseObject(json)
    val channelID: lang.Long = jsonObject.getLong("channelID")
    val categoryID: lang.Long = jsonObject.getLong("categoryID")
    val produceID: lang.Long = jsonObject.getLong("produceID")
    val userID: lang.Long = jsonObject.getLong("userID")
    val country: String = jsonObject.getString("country")
    val province: String = jsonObject.getString("province")
    val city: String = jsonObject.getString("city")
    val network: String = jsonObject.getString("network")
    val source: String = jsonObject.getString("source")
    val browserType: String = jsonObject.getString("browserType")
    val entryTime: lang.Long = jsonObject.getLong("entryTime")
    val leaveTime: lang.Long = jsonObject.getLong("leaveTime")
    ClickLog(
      channelID,
      categoryID,
      produceID,
      userID,
      country,
      province,
      city,
      network,
      source,
      browserType,
      entryTime,
      leaveTime
    )
  }
}