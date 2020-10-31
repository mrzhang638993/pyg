package com.itheima.realprocess.bean

/**
 * 准备原始表的字段信息
 * */
case class ClickLogWide(
                    channelID:Long,
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
                    leaveTime:Long,
                    count:Long,
                    timeStamp:Long,
                    address:String,
                    yearMonth:String,
                    yearMonthDay:String,
                    yearMonthDayHour:String,
                    isNew:Int,
                    isHourNew:Int,
                    isDayNew:Int,
                    isMonthNew:Int
                    )

