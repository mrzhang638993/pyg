package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelRealHot1, ClickLogWide1}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.windowing.time.Time
// 导入隐式转换，其本质是一个函数操作的
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

/**
 * 渠道热点数据的处理操作
 * */
object ChannelRealHotTask1 {

    def process(value:DataStream[ClickLogWide1]):DataStream[ChannelRealHot1]={
        // 执行字段的转换操作 channelId,visited
        val realHotData: DataStream[ChannelRealHot1] = value.map(clickLogWide => {
            // 统计每一个渠道对应的访问的次数信息。
            ChannelRealHot1(clickLogWide.channelId, clickLogWide.count)
        })
        // 执行分组统计数据，根据channelId得到每一个渠道对应的热点数据信息
        val groupValue: KeyedStream[ChannelRealHot1, Tuple] = realHotData.keyBy("channelId")
        // 执行时间窗口统计操作，flink对应的是实时的窗口的，整体的统计是没有什么意义的。
        // 使用时间窗口函数执行数据的统计操作和实现管理操作。
        // channeId:对应的数据可以处理一下的,可以使用渠道的名称实现操作管理的。
        groupValue.timeWindow(Time.seconds(3)).reduce((priv:ChannelRealHot1,next:ChannelRealHot1)=>ChannelRealHot1(priv.channelId,priv.visited+next.visited))
    }
}
