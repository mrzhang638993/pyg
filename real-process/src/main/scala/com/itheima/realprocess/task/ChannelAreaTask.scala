package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelArea, ClickLogWide}
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
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
    // 使用匿名内部类的方式实现sink操作实现
    reduceStream.addSink{
        area=>{
           val tableName="channel_area"
           val clfName="info"
           val rowKey=area.channelID+":"+area.area+":"+area.date
           val  channelIdColumn="channelId"
           val  areaColumn="area"
           val  dateColumn="date"
           val  pvColumn="pv"
           val  uvColumn="uv"
           val  newCountColumn="newCount"
           val  oldCountColumn="oldCount"
           var  totalPvCount=0L
           var  totalUvCount=0L
           var  totalNewCount=0L
           var  totalOldCount=0L
           // 查询hbase数据
           val pvColumnValue: String = HbaseUtil.getData(tableName, clfName, rowKey, pvColumn)
          //   查询hbase数据
           if(pvColumnValue!=null&&StringUtils.isNoneBlank(pvColumnValue)){
             totalPvCount=totalPvCount+pvColumnValue.toLong
           }else{
             totalPvCount=pvColumnValue.toLong
           }
          // 继续获取对应的数值
          val uvColumnValue: String = HbaseUtil.getData(tableName, clfName, rowKey, uvColumn)
          if(uvColumnValue!=null&&StringUtils.isNoneBlank(uvColumnValue)){
            totalUvCount=totalUvCount+uvColumnValue.toLong
          }else{
            totalPvCount=uvColumnValue.toLong
          }
          //  继续获取对应的数值
          val newConutValue: String = HbaseUtil.getData(tableName, clfName, rowKey, newCountColumn)
          if(newConutValue!=null&&StringUtils.isNoneBlank(newConutValue)){
            totalNewCount=totalNewCount+newConutValue.toLong
          }else{
            totalNewCount=newConutValue.toLong
          }
          // 继续获取对应的数据
          val oldCountValue: String = HbaseUtil.getData(tableName, clfName, rowKey, oldCountColumn)
          if(oldCountValue!=null&&StringUtils.isNoneBlank(oldCountValue)){
            totalOldCount=totalOldCount+oldCountValue.toLong
          }else{
            totalOldCount=oldCountValue.toLong
          }
          HbaseUtil.putMapData(tableName,clfName,rowKey,
            Map(
              channelIdColumn->area.channelID,
              areaColumn->area.area,
              dateColumn->area.area,
              pvColumn->totalPvCount,
              uvColumn->totalPvCount,
              newCountColumn->totalNewCount,
              oldCountColumn->totalOldCount
            ))
        }
    }
  }
}
