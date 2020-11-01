package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelArea, ClickLogWide}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ChannelAreaTask  extends  BaseTask[ChannelArea]{
  /**
   * 定义转换操作
   **/
  override def map(clickLogWide: DataStream[ClickLogWide]): DataStream[ChannelArea] = {
    clickLogWide.flatMap[ChannelArea]{
       clickLog=>{
         val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
         List(
           ChannelArea(clickLog.channelID.toString,clickLog.address,clickLog.yearMonth,clickLog.count,clickLog.isMonthNew,clickLog.isNew,isOld(clickLog.isNew,clickLog.isMonthNew)),
           ChannelArea(clickLog.channelID.toString,clickLog.address,clickLog.yearMonthDay,clickLog.count,clickLog.isDayNew,clickLog.isNew,isOld(clickLog.isNew,clickLog.isDayNew)),
           ChannelArea(clickLog.channelID.toString,clickLog.address,clickLog.yearMonthDayHour,clickLog.count,clickLog.isHourNew,clickLog.isNew,isOld(clickLog.isNew,clickLog.isHourNew))
         )
       }
    }
  }

  /**
   * 定义分组操作
   **/
  override def groupBy(mapDataStream: DataStream[ChannelArea]): KeyedStream[ChannelArea, String] = {
    mapDataStream.keyBy(item=>item.channelID+":"+item.area+":"+item.date)
  }

  /**
   * 聚合操作实现
   **/
  override def reduce(windowStream: WindowedStream[ChannelArea, String, TimeWindow]): DataStream[ChannelArea] = {
    windowStream.reduce((priv,next)=>ChannelArea(priv.channelID,priv.area,priv.date,priv.pv+next.pv,priv.uv+next.uv,priv.newCount+next.newCount,priv.oldCount+next.oldCount))
  }

  /**
   * 数据落地到hbase中
   **/
  override def sink2Hbase(reduceStream: DataStream[ChannelArea]): Unit = {

  }
}
