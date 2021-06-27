package com.itheima.realprocess.bean

import com.alibaba.fastjson.JSON

/**
 * 创建样例类对象信息
 * */
case class ClickLog1(
                   var channelId:String,
                   var categoryId:String,
                   var produceId:String,
                   var country:String,
                   var province:String,
                   var city:String,
                   var network:String,
                   var source:String,
                   var browerType:String,
                   var entryTime:String,
                   var leaveTime:String,
                   var userId:String
                   )

/**
 * 样例类的伴生对象
 * */
object ClickLog1{
   def apply(jsonStr:String):ClickLog1={
     val log: ClickLog1 = JSON.parseObject(jsonStr, classOf[ClickLog1])
     log
   }
}
