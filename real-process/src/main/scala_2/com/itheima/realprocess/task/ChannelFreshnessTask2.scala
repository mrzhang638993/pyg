package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelFreshness1, ClickLogWide1}
import com.itheima.realprocess.util.HbaseUtils1
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable
/*
    重写代码设计的相关的操作和实现机制。
* */
object ChannelFreshnessTask2  extends BaseTask1[ChannelFreshness1]{
  override def map(clickLogWide: DataStream[ClickLogWide1]): DataStream[ChannelFreshness1] = {
    val oldAndFreshUser: DataStream[ChannelFreshness1] = clickLogWide.flatMap{
      clickLogWide =>
        List(
          ChannelFreshness1(clickLogWide.channelId, clickLogWide.yearMonthDay, clickLogWide.isDayNew, 1 - clickLogWide.isDayNew)
          , ChannelFreshness1(clickLogWide.channelId, clickLogWide.yearMonth, clickLogWide.isMonthNew, 1 - clickLogWide.isMonthNew)
          , ChannelFreshness1(clickLogWide.channelId, clickLogWide.yearMonthDayHour, clickLogWide.isHourNew, 1 - clickLogWide.isHourNew)
        )
    }
    oldAndFreshUser
  }

  override def groupBy(value: DataStream[ChannelFreshness1]): KeyedStream[ChannelFreshness1,String] = {
    value.keyBy(fress=>(fress.channelId+":"+fress.date))
  }

  override def timeWindow(value: KeyedStream[ChannelFreshness1,String]): WindowedStream[ChannelFreshness1,String, TimeWindow] = {
     value.timeWindow(Time.seconds(3))
  }

  override def reduce(value: WindowedStream[ChannelFreshness1,String, TimeWindow]): DataStream[ChannelFreshness1] = {
    value.reduce((priv,next)=>ChannelFreshness1(priv.channelId,priv.date,priv.newCount+next.newCount,priv.oldCount+next.oldCount))
  }

  override def sinkToHbase(value: DataStream[ChannelFreshness1]): Unit = {
    value.addSink(new SinkFunction[ChannelFreshness1] {
      override def invoke(value: ChannelFreshness1, context: SinkFunction.Context[_]): Unit ={
        val tableName:String="channel_freshness"
        val clfName:String="info"
        val rowkey=value.channelId+":"+value.date
        //  查询new历史数据
        val channelIdColumn:String="channelId"
        val dateColumn:String="date"
        val newCountColumn:String="newCount"
        val oldCountColumn:String="oldCount"
        val mapValue: Map[String, String] = HbaseUtils1.getMapData(tableName, rowkey, clfName, List(newCountColumn, oldCountColumn))
        if(mapValue!=null){
          val newCount:String = mapValue.getOrElse(newCountColumn, 0).toString
          val oldCount: String = mapValue.getOrElse(oldCountColumn, 0).toString
          HbaseUtils1.putMapData(tableName,rowkey,clfName,mutable.Map(newCountColumn->(value.newCount+newCount.toLong).toString,oldCountColumn->(value.oldCount+oldCount.toLong).toString,channelIdColumn->value.channelId,dateColumn->value.date))
        }else{
          HbaseUtils1.putMapData(tableName,rowkey,clfName,mutable.Map(newCountColumn->value.newCount.toString,oldCountColumn->value.oldCount.toString,channelIdColumn->value.channelId,dateColumn->value.date))
        }
      }
    })
  }
}
