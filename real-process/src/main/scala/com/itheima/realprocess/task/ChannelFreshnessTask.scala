package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelFreshness1, ClickLogWide}
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
object ChannelFreshnessTask extends  BaseTask[ChannelFreshness1]{
  /**
   * 定义转换操作
   **/
  override def map(clickLogWide: DataStream[ClickLogWide]): DataStream[ChannelFreshness1] = {
    clickLogWide.flatMap {
      clickLog =>
        // 判断是新用户还是老用户的信息。
        List(
          ChannelFreshness1(clickLog.channelID.toString, clickLog.yearMonth, clickLog.isNew, isOld(clickLog.isNew, clickLog.isMonthNew)),
          ChannelFreshness1(clickLog.channelID.toString, clickLog.yearMonthDay, clickLog.isNew, isOld(clickLog.isNew, clickLog.isDayNew)),
          ChannelFreshness1(clickLog.channelID.toString, clickLog.yearMonthDayHour, clickLog.isNew, isOld(clickLog.isNew, clickLog.isHourNew))
        )
    }
  }

  /**
   * 定义分组操作
   **/
  override def groupBy(mapDataStream: DataStream[ChannelFreshness1]): KeyedStream[ChannelFreshness1, String] = {
    mapDataStream.keyBy(freshness => freshness.channelID + freshness.date)
  }

  /**
   * 聚合操作实现
   **/
  override def reduce(windowStream: WindowedStream[ChannelFreshness1, String, TimeWindow]): DataStream[ChannelFreshness1] = {
    windowStream.reduce((priv, next) => ChannelFreshness1(priv.channelID, priv.date, priv.newCount + next.newCount, priv.oldCount + next.oldCount))
  }

  /**
   * 数据落地到hbase中
   **/
  override def sink2Hbase(reduceStream: DataStream[ChannelFreshness1]): Unit = {
    reduceStream.addSink(new SinkFunction[ChannelFreshness1] {
      override def invoke(value: ChannelFreshness1): Unit = {
        // 定义变量
        val tableName = "channel_freshness"
        val rowKey = value.channelID + ":" + value.date
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
