package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelRealHot, ClickLogWide}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 进行数据的转换操作
 * 1.字段转换
 * 2.分组
 * 3.时间窗口
 * 4.聚合
 * 5.落地Hbase
 * */
object ChannelRealHotTask {

  /**
   * 字段:channelID
   * 字段：visited 访问时间
   * */
  def process(clickLogWide:DataStream[ClickLogWide]): DataStream[ChannelRealHot] ={
    // 进行数据转换操作
    val channelRealHot: DataStream[ChannelRealHot] = clickLogWide.map {
      clickLogWide => {
        ChannelRealHot(clickLogWide.channelID.toString, clickLogWide.count)
      }
    }
    //分组聚合结果操作
    val keyByValues: KeyedStream[ChannelRealHot, String] = channelRealHot.keyBy(_.channelID)
    // 时间窗口
    val windowStream: WindowedStream[ChannelRealHot, String, TimeWindow] = keyByValues.timeWindow(Time.seconds(3))
    // 聚合操作
    val reduceValues: DataStream[ChannelRealHot] = windowStream.reduce((priv, next) => ChannelRealHot(priv.channelID, priv.visited + next.visited))
    reduceValues
  }
}
