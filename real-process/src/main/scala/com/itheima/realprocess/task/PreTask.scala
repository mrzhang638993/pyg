package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ClickLogWide, Message}
import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

object PreTask {
  //  数据预处理操作，对数据执行操作实现
  def process(waterValue: DataStream[Message]): DataStream[ClickLogWide] = {
    waterValue.map {
      msg => {
        // 进行时间和地区的转换操作实现
        val yearMonth: String = FastDateFormat.getInstance("yyyyMM").format(msg.timeStamp)
        val yearMonthDay: String = FastDateFormat.getInstance("yyyyMMdd").format(msg.timeStamp)
        val yearMonthDayHour: String = FastDateFormat.getInstance("yyyyMMddHH").format(msg.timeStamp)
        // 执行地区的操作实现
        val address = msg.clickLog.country + msg.clickLog.city
        ClickLogWide(
          msg.clickLog.channelID,
          msg.clickLog.categoryID,
          msg.clickLog.produceID,
          msg.clickLog.userID,
          msg.clickLog.country,
          msg.clickLog.province,
          msg.clickLog.city,
          msg.clickLog.network,
          msg.clickLog.source,
          msg.clickLog.browserType,
          msg.clickLog.entryTime,
          msg.clickLog.leaveTime,
          msg.count,
          msg.timeStamp,
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
  }
}
