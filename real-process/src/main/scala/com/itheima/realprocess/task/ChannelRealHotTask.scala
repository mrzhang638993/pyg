package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelRealHot, ClickLogWide}
import com.itheima.realprocess.util.HbaseUtil
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
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
  def process(clickLogWide:DataStream[ClickLogWide]):Unit={
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
    // 数据落地到hbase
    reduceValues.addSink(new RichSinkFunction[ChannelRealHot] {
      override def invoke(value: ChannelRealHot): Unit = {
         val  tableName="channel"
         val clfName="info"
         val channelIdColumn="channelId"
         val visitedColumn="visited"
         val rowKey=value.channelID
        val visitedCount: String = HbaseUtil.getData(tableName, clfName, rowKey, visitedColumn)
          if(visitedColumn==null||visitedColumn.isEmpty){
            // 不做任何的处理，直接写入到hbase中的
            HbaseUtil.putMapData(tableName,clfName,rowKey,Map(channelIdColumn->value.channelID,visitedColumn->value.visited))
          }else{
            // hbase中存在数据的话，需要执行更新操作实现的
            HbaseUtil.putData(tableName,clfName,rowKey,visitedColumn,(visitedCount.toLong+value.visited).toString)
          }
      }
    })
  }
}
