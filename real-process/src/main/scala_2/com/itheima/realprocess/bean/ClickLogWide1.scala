package com.itheima.realprocess.bean

/**
 * 点击流日志的拓宽数据
 * */
case class ClickLogWide1(
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
                         var userId:String,
                         var count:Long,
                         var timeStamp:String,
                         var address:String,
                         var yearMonth:String,
                         var yearMonthDay:String,
                         var yearMonthDayHour:String,
                         var isNew:Int, // 历史表中没有数据的话，对应的就不是new的数据的。0代表的是新的数据,默认的是新的数据的，1代表的是旧的数据的。
                         var isHourNew:Int,
                         var isDayNew:Int,
                         var isMonthNew:Int
                       )

object ClickLogWide1{
   def apply(yearMonth:String,yearMonthDay:String,yearMonthDayHour:String,address:String,message1: Message1): ClickLogWide1 = {
     new ClickLogWide1(
       message1.log.channelId,
       message1.log.categoryId,
       message1.log.produceId,
       message1.log.country,
       message1.log.province,
       message1.log.city,
       message1.log.network,
       message1.log.source,
       message1.log.browerType,
       message1.log.entryTime,
       message1.log.leaveTime,
       message1.log.userId,
       message1.count,
       message1.timeStamp.toString,
       address,
       yearMonth,
       yearMonthDay,
       yearMonthDayHour,
       0,
       0,
       0,
       0
     )
   }
}