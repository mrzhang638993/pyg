package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelFreshness, ClickLogWide}
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 用户的新鲜度任务测试
 **/
object ChannelFreshnessTask {
  def process(clickLogWide: DataStream[ClickLogWide]) = {
    // 数据转换成为多个时间维度的数据
    val mapDataStream: DataStream[ChannelFreshness] = clickLogWide.flatMap {
      clickLog =>
        // 判断是新用户还是老用户的信息。
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
        List(
          ChannelFreshness(clickLog.channelID.toString, clickLog.yearMonth, clickLog.isNew, isOld(clickLog.isNew, clickLog.isMonthNew)),
          ChannelFreshness(clickLog.channelID.toString, clickLog.yearMonthDay, clickLog.isNew, isOld(clickLog.isNew, clickLog.isDayNew)),
          ChannelFreshness(clickLog.channelID.toString, clickLog.yearMonthDayHour, clickLog.isNew, isOld(clickLog.isNew, clickLog.isHourNew))
        )
    }
    // 分组
    val keyedStream: KeyedStream[ChannelFreshness, String] = mapDataStream.keyBy(freshness => freshness.channelID + freshness.date)
    // 时间窗口
    val windowStream: WindowedStream[ChannelFreshness, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))
    // 聚合操作
    val reduceDataStream: DataStream[ChannelFreshness] = windowStream.reduce((priv, next) => ChannelFreshness(priv.channelID, priv.date, priv.newCount + next.newCount, priv.oldCount + next.oldCount))
    // 落地hbase
    reduceDataStream.addSink(new SinkFunction[ChannelFreshness] {
      override def invoke(value: ChannelFreshness): Unit = {
        // 定义变量
        val tableName = "channel_freshness"
        val clfName = "info"
        val rowKey = value.channelID + ":" + value.date
        val channelIdColumn = "channelId"
        val dateColumn = "date"
        val newCountColumn = "newCount"
        val oldCountColumn = "oldCount"
        // 查询历史数据
        val mapData: Map[String, String] = HbaseUtil.getMapData(tableName, clfName, rowKey, List(channelIdColumn, dateColumn, newCountColumn, oldCountColumn))
        //  进行数据相加操作
        var newCount = 0L
        var oldCount = 0L
        if (mapData != null && StringUtils.isNotBlank(mapData.getOrElse(newCount.toString, ""))) {
          newCount = mapData.get(newCountColumn).get.toLong + value.newCount
        } else {
          newCount = value.newCount
        }
        if (mapData != null && StringUtils.isNotBlank(mapData.getOrElse(oldCount.toString, ""))) {
          oldCount = mapData.get(oldCountColumn).get.toLong + value.oldCount
        } else {
          // 保存入库操作
          oldCount = value.oldCount
        }
        HbaseUtil.putMapData(tableName, clfName, rowKey, Map(channelIdColumn -> value.channelID, dateColumn -> value.date, newCountColumn -> newCount, oldCountColumn -> oldCount))
      }
    })
  }
}