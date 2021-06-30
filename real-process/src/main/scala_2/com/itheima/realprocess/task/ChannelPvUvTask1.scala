package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelPvUv1, ClickLogWide1}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 统计渠道的pvuv的数量信息的。
 * pv:代表的是网页的访问量，uv对应的是独立的用户数量
 * 统计的是渠道的pv以及uv数据的。计算的是小时维度的pv以及uv参数
 * */
object ChannelPvUvTask1 {

  // map对应的是一种转化成为一种的，flatmap对应的是一个数据转换成为3个的
  def process(preTaskData: DataStream[ClickLogWide1]): DataStream[ChannelPvUv1] ={
    // 得到数据信息，执行数据的加减操作的
    val channelPvUv: DataStream[ChannelPvUv1] = preTaskData.flatMap {
      clickLogWide => {
        // isNew:对应的代表的是一个新的用户信息，对应的数值为1，老用户第对应的是0的
        List(
          //日数据的pvuv操作
          ChannelPvUv1(clickLogWide.channelId, clickLogWide.yearMonthDayHour, clickLogWide.count, clickLogWide.isHourNew)
         //天数据的pv以及uv操作
          ,ChannelPvUv1(clickLogWide.channelId, clickLogWide.yearMonthDay, clickLogWide.count, clickLogWide.isDayNew)
         //月数据的pv以及uv操作
          ,ChannelPvUv1(clickLogWide.channelId, clickLogWide.yearMonth, clickLogWide.count, clickLogWide.isMonthNew)
        )
      }
    }
    //将数据分成多种流数据执行流的操作处理和实现管理
    // flink数据处理的时候需要增加窗口操作的,这个是基本的知识体系的。
    channelPvUv.keyBy("channelId","yearDayMonthHour")
      .timeWindow(Time.seconds(3))
      .reduce((priv,next)=>ChannelPvUv1(priv.channelId,priv.yearDayMonthHour,priv.pv+next.pv,priv.uv+next.uv))
  }
}
