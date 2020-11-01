package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelBrowser, ChannelNetwork, ClickLogWide}
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 运营商角度进行分析操作
 * */
object ChannelBrowserTask  extends  BaseTask[ChannelBrowser]{
  /**
   * 定义转换操作
   **/
  override def map(clickLogWide: DataStream[ClickLogWide]): DataStream[ChannelBrowser] = {
    clickLogWide.flatMap{
      clickLog=>{
        // 求解oldCount
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
        List(
          ChannelBrowser(clickLog.channelID.toString,clickLog.browserType,clickLog.yearMonth,clickLog.count,clickLog.isMonthNew,clickLog.isNew,isOld(clickLog.isNew,clickLog.isMonthNew)),
          ChannelBrowser(clickLog.channelID.toString,clickLog.browserType,clickLog.yearMonthDay,clickLog.count,clickLog.isDayNew,clickLog.isNew,isOld(clickLog.isNew,clickLog.isDayNew)),
          ChannelBrowser(clickLog.channelID.toString,clickLog.browserType,clickLog.yearMonthDayHour,clickLog.count,clickLog.isHourNew,clickLog.isNew,isOld(clickLog.isNew,clickLog.isHourNew))
        )
      }
    }
  }

  /**
   * 定义分组操作
   **/
  override def groupBy(mapDataStream: DataStream[ChannelBrowser]): KeyedStream[ChannelBrowser, String] = {
    mapDataStream.keyBy{
      item=>item.channelID+":"+item.browserType+":"+item.date
    }
  }

  /**
   * 聚合操作实现
   **/
  override def reduce(windowStream: WindowedStream[ChannelBrowser, String, TimeWindow]): DataStream[ChannelBrowser] = {
    windowStream.reduce((priv,next)=>ChannelBrowser(priv.channelID,priv.browserType,priv.date,priv.pv+next.pv,priv.uv+next.uv,priv.newCount+next.newCount,priv.oldCount+next.oldCount))
  }

  /**
   * 数据落地到hbase中
   **/
  override def sink2Hbase(reduceStream: DataStream[ChannelBrowser]): Unit = {
     //  数据入库保存到hbase数据集中
    reduceStream.addSink{
      value=>{
        val rowKey =value.channelID+":"+value.browserType+":"+value.date
        val tableName = "channel_browser"
        val browserTypeColumn="browserType"
        // 查询历史数据
        val mapData: Map[String, String] = HbaseUtil.getMapData(tableName, clfName, rowKey, List(pvColumn, uvColumn, newCountColumn, oldCountColumn))
        //  进行数据相加操作
        var newCount = 0L
        var oldCount = 0L
        if (mapData != null && StringUtils.isNotBlank(mapData.getOrElse(newCount.toString, ""))) {
          newCount = mapData.get(newCountColumn).get.toLong + value.newCount
        } else {
          newCount = value.newCount
        }
        if (mapData != null && StringUtils.isNotBlank(mapData.getOrElse(oldCount.toString, ""))) {
          oldCount = mapData.get(oldCountColumn).get.toLong + value.oldCount
        } else {
          // 保存入库操作
          oldCount = value.oldCount
        }
        // 判断pv的数量
        if (mapData != null && StringUtils.isNotBlank(mapData.getOrElse(totalPvCount.toString, ""))) {
          totalPvCount = mapData.get(pvColumn).get.toLong + value.pv
        } else {
          // 保存入库操作
          totalPvCount = value.pv
        }
        // 判断uv的数量信息
        if (mapData != null && StringUtils.isNotBlank(mapData.getOrElse(totalUvCount.toString, ""))) {
          totalUvCount = mapData.get(uvColumn).get.toLong + value.uv
        } else {
          // 保存入库操作
          totalUvCount = value.uv
        }
        HbaseUtil.putMapData(tableName, clfName, rowKey, Map(
          channelIdColumn->value.channelID,
          browserTypeColumn->value.browserType,
          dateColumn->value.date,
          pvColumn -> totalPvCount,
          uvColumn -> totalUvCount,
          newCountColumn -> newCount,
          oldCountColumn -> oldCount))
      }
    }
  }
}
