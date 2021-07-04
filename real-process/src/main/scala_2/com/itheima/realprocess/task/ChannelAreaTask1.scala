package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelArea1, ClickLogWide1}
import com.itheima.realprocess.util.HbaseUtils1
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable

/**
 * 分析渠道地域的行为特征。对应的区分键是渠道以及地域的信息。
 * */
object ChannelAreaTask1 extends BaseTask1[ChannelArea1]{
  override def map(clickLogWide: DataStream[ClickLogWide1]): DataStream[ChannelArea1] = {
    clickLogWide.flatMap(clickLog=>{
      List(
        //  这里面只是计算小时级别的数据的，其他的包括天级别的数据以及月级别的数据暂时不管了。
        // clickLog.isMonthNew 对应的是uv的数量的，clickLog.isMonthNew对应的是新用户的数量的，1-clickLog.isMonthNew对应的是老用户的数量的。
        ChannelArea1(clickLog.channelId,clickLog.address,clickLog.yearMonth,1,clickLog.isMonthNew,clickLog.isMonthNew,1-clickLog.isMonthNew),
        ChannelArea1(clickLog.channelId,clickLog.address,clickLog.yearMonthDay,1,clickLog.isDayNew,clickLog.isDayNew,1-clickLog.isDayNew),
        ChannelArea1(clickLog.channelId,clickLog.address,clickLog.yearMonthDayHour,1,clickLog.isHourNew,clickLog.isHourNew,1-clickLog.isHourNew)
      )
    })
  }

  override def groupBy(value: DataStream[ChannelArea1]): KeyedStream[ChannelArea1, String] = {
      // 根据渠道,地域以及时间进行分析和操作实现。
      value.keyBy(channel=>channel.channelId+":"+channel.area+":"+channel.date)
  }

  /*override def timeWindow(value: KeyedStream[ChannelArea1, String]): WindowedStream[ChannelArea1, String, TimeWindow] = {
    value.timeWindow(Time.seconds(3))
  }*/

  override def reduce(value: WindowedStream[ChannelArea1, String, TimeWindow]): DataStream[ChannelArea1] = {
     value.reduce((priv,next)=>ChannelArea1(priv.channelId,priv.area,priv.date,priv.pv+next.pv,priv.uv+next.uv,priv.newCount+next.newCount,priv.oldCount+next.oldCount))
  }

  //数据落地到hbase中
  override def sinkToHbase(value: DataStream[ChannelArea1]): Unit = {
    value.addSink(new SinkFunction[ChannelArea1]{
      override def invoke(value: ChannelArea1, context: SinkFunction.Context[_]): Unit = {
          val tableNameStr:String="channel_area"
          val clfName:String="info"
          val rowKey=value.channelId+":"+value.area+":"+value.date
          val channelIdColumn:String="channelId"
          val areaColumn:String="area"
          val dateColumn:String="date"
          val pvColumn:String="pv"
          val uvColumn:String="uv"
          val newCountColumn:String="newCount"
          val oldCountColumn:String="oldCount"
          var pvCount:Long=value.pv
          var uvCount:Long=value.uv
          var newCount:Long=value.newCount
          var oldCount:Long=value.oldCount
          val mapData: Map[String, String] = HbaseUtils1.getMapData(tableNameStr, rowKey, clfName, List(pvColumn, uvColumn, newCountColumn, oldCountColumn))
          if(mapData!=null){
            val pvValue: Option[String] = mapData.get(pvColumn)
            if(!pvValue.isEmpty){
              pvCount+=pvValue.get.toLong
            }
            val uvValue: Option[String] = mapData.get(uvColumn)
            if(!uvValue.isEmpty){
              uvCount+=uvValue.get.toLong
            }
            val oldCountValue: Option[String] = mapData.get(oldCountColumn)
            if(!oldCountValue.isEmpty){
              oldCount+=oldCountValue.get.toLong
            }
            val newCountValue: Option[String] = mapData.get(newCountColumn)
            if(!newCountValue.isEmpty){
              newCount+=newCountValue.get.toLong
            }
          }
        HbaseUtils1.putMapData(tableNameStr,rowKey,clfName,mutable.Map(
          channelIdColumn->value.channelId,
          areaColumn->value.area,
          dateColumn->value.date,
          pvColumn->pvCount,
          uvCount->uvCount,
          oldCountColumn->oldCount,
          newCountColumn->newCount
        ))
      }
    })
  }
}
