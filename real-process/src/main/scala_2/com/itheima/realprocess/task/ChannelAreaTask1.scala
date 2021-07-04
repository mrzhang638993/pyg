package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelArea1, ClickLogWide1}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

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

  override def sinkToHbase(value: DataStream[ChannelArea1]): Unit = {

  }
}
