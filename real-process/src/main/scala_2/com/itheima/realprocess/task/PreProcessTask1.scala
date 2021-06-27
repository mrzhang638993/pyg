package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ClickLogWide1, Message1}
import com.itheima.realprocess.util.HbaseUtils1
import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable

/**
 * 预处理任务对象信息
 * */
object PreProcessTask1 {

  /**
   * 更改对应的ClickLogWide1的服务状态信息
   * */
  def changeWideStatus(wide: ClickLogWide1): (Int,Int,Int,Int) = {
      var isNew=0
      var isHourNew=0
      var isDayNew=0
      var isMonthNew=0
      val tableName:String="user_history"
      val cfName:String="info"
      val rowKey:String=wide.userId+":"+wide.channelId
      val userIdColumn="userId"
      val channelIdColumn="channelId"
      val lastVisitedColumn="lastVisitedTime"
      val userId: String = HbaseUtils1.getData(tableName, rowKey, cfName, userIdColumn)
      //val channelId: String = HbaseUtils1.getData(tableName, rowKey, cfName, channelIdColumn)
      val lastVisited: String = HbaseUtils1.getData(tableName, rowKey, cfName, lastVisitedColumn)
      if(StringUtils.isBlank(userId)){
        // 如果没有用户记录的话,对应的用户id的记录全部为空。对应的是一个新的用户的。
        isNew=1
        isHourNew=1
        isDayNew=1
        isMonthNew=1
        HbaseUtils1.putMapData(tableName,rowKey,cfName,mutable.Map(userIdColumn->wide.userId,channelIdColumn->wide.channelId,lastVisitedColumn->wide.timeStamp))
      }else{
        isNew=0 // 代表是一个老用户的
        // 判断处于那种时间的级别的数据的.比对时间戳信息确定是那种级别的老用户的。
        // 当前时间比历史时间大的话，对应的是新的用户的？具体的查看是在那个维度上大于或者是等于的比较的。
        //  当前时间相较于原来的时间大的话，对应的是新的用户的，否则是老用户的。这个逻辑上是存在一定的问题的,需要排查的。
        isHourNew=compareDate(wide.timeStamp.toLong,lastVisited.toLong,"yyyyMMddHH")
        isDayNew=compareDate(wide.timeStamp.toLong,lastVisited.toLong,"yyyyMMdd")
        isMonthNew=compareDate(wide.timeStamp.toLong,lastVisited.toLong,"yyyyMMdd")
        // 更新数据信息
        HbaseUtils1.putData(tableName,rowKey,cfName,lastVisitedColumn,wide.timeStamp)
      }
    (isNew,isHourNew,isDayNew,isMonthNew)
  }

  /**
   * 比较相关的时间戳数据信息。format对应的是时间格式信息。
   * */
  def  compareDate(currentTime:Long,historyTime:Long,format:String):Int={
    val currentTimeStr: String = timestamp2Str(currentTime, format)
    val historyTimeStr: String = timestamp2Str(historyTime, format)
    var result: Int = currentTimeStr.compareTo(historyTimeStr)
    result=if(result>0) 1 else 0
    result
  }

  /**
   * 时间戳
   * */
  def timestamp2Str(timestamp:Long,format:String):String={
      FastDateFormat.getInstance(format).format(timestamp)
  }

  def process(waterStream:DataStream[Message1]): DataStream[ClickLogWide1] ={
    val value: DataStream[ClickLogWide1] = waterStream.map {
      message => {
        val yearMonth: String = FastDateFormat.getInstance("yyyyMM").format(message.timeStamp)
        val yearMonthDay: String = FastDateFormat.getInstance("yyyyMMdd").format(message.timeStamp)
        val yearMonthDayHour: String = FastDateFormat.getInstance("yyyyMMddHH").format(message.timeStamp)
        //  转换地区实现实现
        val address = message.log.country + message.log.province + message.log.city
        val wide: ClickLogWide1 = ClickLogWide1(yearMonth, yearMonthDay, yearMonthDayHour, address, message)
        val tuple: (Int, Int, Int, Int) = changeWideStatus(wide)
        wide.isNew=tuple._1
        wide.isHourNew=tuple._2
        wide.isDayNew=tuple._3
        wide.isMonthNew=tuple._4
        wide
      }
    }
    //  处理完成了之后,对于每一个数据需要更新对应的新以及旧的业务标识信息的。
    value
  }
}
