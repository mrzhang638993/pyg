package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ClickLogWide, Message}
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils
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
        val isNewTuple: (Int, Int, Int, Int) = isNewProcess(msg)
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
          isNewTuple._1,
          isNewTuple._2,
          isNewTuple._3,
          isNewTuple._4
        )
      }
    }
  }

  //  定义私有方法，处理isNew字段的信息？
  def isNewProcess(msg: Message): (Int, Int, Int, Int) = {
    // 定义初始化的字段。默认值为0
    var isNew = 0
    var isHourNew = 0
    var isDayNew = 0
    var isMonthNew = 0
    // 从hbase中进行查询，操作逻辑判断用户是否是新的。如果没有记录的话,对应的是新用户的。
    //hbase的表名称
    val tableName = "user_history"
    // 列族名称
    val clfName = "info"
    // 用户对应的rowkey信息
    val rowKey = msg.clickLog.userID + ":" + msg.clickLog.channelID
    // hbase查询的列信息
    val userIdColumn = "userId"
    //保存多个列族信息入库进行操作
    val channelIdColumn = "channelid"
    val lastVisitedTimeColumn = "lastVisitedTime"
    val userId: String = HbaseUtil.getData(tableName, clfName, rowKey, userIdColumn)
    val lastVisitedTime: String = HbaseUtil.getData(tableName, clfName, rowKey, lastVisitedTimeColumn)
    if (StringUtils.isBlank(userId)) {
      // 对应的是新的用户信息的
      isNew = 1
      isHourNew = 1
      isDayNew = 1
      isMonthNew = 1
      // 保存数据到user_history数据表中
      HbaseUtil.putMapData(tableName, clfName, rowKey,
        Map(userIdColumn -> msg.clickLog.userID,
          channelIdColumn -> msg.clickLog.channelID,
          lastVisitedTimeColumn -> msg.timeStamp))
    } else {
      isNew = 0
      // 其他的时间段是需要根据时间进行判断操作实现的
      // 查询处理啊历史时间数据
      isDayNew= compareTime(msg.timeStamp, lastVisitedTime.toLong, "yyyyMMdd")
      isHourNew= compareTime(msg.timeStamp, lastVisitedTime.toLong, "yyyyMMddHH")
      isHourNew= compareTime(msg.timeStamp, lastVisitedTime.toLong, "yyyyMM")
      //更新用户的时间戳信息。将用户的最新访问时间信息保存到访问历史记录中。需要更新旧的用户的访问信息到历史记录中的。
      HbaseUtil.putData(tableName, clfName, rowKey,lastVisitedTimeColumn,lastVisitedTime)
    }
    // 下面需要查询出来，年份，月份，天，以及小时对应的new信息的。
    (isNew, isHourNew, isDayNew, isMonthNew)
  }

  /**
   * 进行时间的比对操作实现。确定在哪一个层面是新的用户信息的。
   * 逻辑存在问题的。在秒数大的情况下，分钟也是大的，或者谁天数也是大的，月份也是大的，年份也是大的。
   **/
  def compareTime(currentTimeStamp: Long, timeStamp: Long, formatStr: String): Int = {
    val currentStr: String = timeStampToStr(currentTimeStamp, formatStr)
    val historyStr: String = timeStampToStr(timeStamp, formatStr)
    if (currentStr.compareTo(historyStr) > 0) {
      1
    } else {
      0
    }
  }

  /**
   * 时间格式转换成为字符串信息
   **/
  def timeStampToStr(timeStamp: Long, formatStr: String): String = {
    FastDateFormat.getInstance(formatStr).format(timeStamp)
  }
}
