package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelPvUv, ClickLogWide}
import com.itheima.realprocess.util.HbaseUtil
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object ChannelPvUvTask {
    /**
     * 数据落地到hbase中进行实时查询操作
     * */
    def  process(clickLogWide:DataStream[ClickLogWide]): Unit ={
        //  需要返回年，月，日的时间数据的。
        val clivkPvUvDatas: DataStream[ChannelPvUv] = clickLogWide.flatMap{
            //  pv，uv的数据需要重新确定操作处理的，数据存在问题的。一份数据最终达到三个数据的。
            clickLog => List(
                ChannelPvUv(clickLog.channelID.toString, clickLog.yearMonthDayHour, clickLog.count, clickLog.isHourNew),
                ChannelPvUv(clickLog.channelID.toString, clickLog.yearMonth, clickLog.count, clickLog.isMonthNew),
                ChannelPvUv(clickLog.channelID.toString, clickLog.yearMonthDay, clickLog.count, clickLog.isDayNew)
            )
        }
        // 分组操作实现,分组规则channelID+yearMonthDayHour的数据信息
        val value: KeyedStream[ChannelPvUv, String] = clivkPvUvDatas.keyBy {
            clickPvUv => clickPvUv.channelID + clickPvUv.yearMonthDayHour
        }
        // 窗口定义操作
        val windowValue: WindowedStream[ChannelPvUv, String, TimeWindow] = value.timeWindow(Time.seconds(3))
        // 实现自定义函数操作实现
        val windowReduceValue: DataStream[ChannelPvUv] = windowValue.reduce((priv, next) => ChannelPvUv(priv.channelID, priv.yearMonthDayHour, priv.pv + next.pv, priv.uv + next.uv))
        // 实现数据落地操作管理
        windowReduceValue.addSink(new SinkFunction[ChannelPvUv] {
            override def invoke(value: ChannelPvUv): Unit = {
              //  定义hbase的数据表和将对应的数据落地到表中的
               val tableName="channelPvUv"
               val clfName="info"
               val channelIdColumn="channelId"
               val yearMonthDayHourColumn="yearMonthDayHour"
               val pvColumn="pv"
               val uvColumn="uv"
               val rowKey=value.channelID+":"+value.yearMonthDayHour
               // 首先查询hbase获取到数据信息,精心数据的比对操作
               val columnsValue: Map[String, String] = HbaseUtil.getMapData(tableName, clfName, rowKey, List(channelIdColumn,
                   yearMonthDayHourColumn,
                   pvColumn,
                   uvColumn)
               )
               //  获取column的列数据信息进行操作实现
               if(columnsValue==null||columnsValue.isEmpty){
                   // 数据列不存在或者是空的话，所有的数据都是新的，需要保存数据到hbase中的
                   HbaseUtil.putMapData(tableName,clfName,rowKey,Map(
                       channelIdColumn->value.channelID,
                       yearMonthDayHourColumn->value.yearMonthDayHour,
                       pvColumn->value.pv,
                       uvColumn->value.uv
                   ))
               }else{
                   // 存在数据的话，需要执行更新操作实现
                   HbaseUtil.putMapData(tableName,clfName,rowKey,Map(
                       channelIdColumn->value.channelID,
                       yearMonthDayHourColumn->value.yearMonthDayHour,
                       pvColumn->(value.pv+columnsValue.get(pvColumn).get),
                       uvColumn->(value.uv+columnsValue.get(uvColumn).get)
                   ))
               }
            }
        })
    }
}
