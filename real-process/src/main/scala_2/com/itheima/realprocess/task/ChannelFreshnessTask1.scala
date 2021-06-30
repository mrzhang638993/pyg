package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelFreshness1, ClickLogWide1}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

//用户新鲜度操作。对应的是新老用户对应的比例数据的
//统计渠道对应的新老用户的数量信息的。
object ChannelFreshnessTask1 {

    def process(preTaskData: DataStream[ClickLogWide1]): DataStream[ChannelFreshness1] ={
      val oldAndFreshUser: DataStream[ChannelFreshness1] = preTaskData.flatMap{
        clickLogWide =>
          List(
              ChannelFreshness1(clickLogWide.channelId, clickLogWide.yearMonthDay, clickLogWide.isDayNew, 1 - clickLogWide.isDayNew)
            , ChannelFreshness1(clickLogWide.channelId, clickLogWide.yearMonth, clickLogWide.isMonthNew, 1 - clickLogWide.isMonthNew)
            , ChannelFreshness1(clickLogWide.channelId, clickLogWide.yearMonthDayHour, clickLogWide.isHourNew, 1 - clickLogWide.isHourNew)
          )
      }
      oldAndFreshUser.keyBy("channelId","date").timeWindow(Time.seconds(3))
        .reduce((priv,next)=>ChannelFreshness1(priv.channelId,priv.date,priv.newCount+next.newCount,priv.oldCount+next.oldCount))
    }
}
