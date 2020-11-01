package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelFreshness, ClickLogWide}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

/**
 * 用户的新鲜度任务测试
 * */
object ChannelFreshnessTask {

   def process(clickLogWide:DataStream[ClickLogWide])={
      // 数据转换成为多个时间维度的数据
      val dateValue: DataStream[List[ChannelFreshness]] = clickLogWide.map {
         clickLog =>
            List(
               ChannelFreshness(clickLog.channelID.toString, clickLog.yearMonth, 1L, 1L),
               ChannelFreshness(clickLog.channelID.toString, clickLog.yearMonthDay, 1L, 1L),
               ChannelFreshness(clickLog.channelID.toString, clickLog.yearMonthDayHour, 1L, 1L)
            )
      }
      //  转换
      // 分组
      // 时间窗口
      // 聚合操作
      // 落地hbase
   }
}
