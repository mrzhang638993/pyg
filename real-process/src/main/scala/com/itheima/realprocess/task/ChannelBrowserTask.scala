package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelBrowser,ClickLogWide}
import com.itheima.realprocess.util.HbaseUtil
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 运营商角度进行分析操作
 **/
object ChannelBrowserTask extends BaseTask[ChannelBrowser] {
  /**
   * 定义转换操作
   **/
  override def map(clickLogWide: DataStream[ClickLogWide]): DataStream[ChannelBrowser] = {
    clickLogWide.flatMap {
      clickLog => {
        // 求解oldCount
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
        List(
          ChannelBrowser(clickLog.channelID.toString, clickLog.browserType, clickLog.yearMonth, clickLog.count, clickLog.isMonthNew, clickLog.isNew, isOld(clickLog.isNew, clickLog.isMonthNew)),
          ChannelBrowser(clickLog.channelID.toString, clickLog.browserType, clickLog.yearMonthDay, clickLog.count, clickLog.isDayNew, clickLog.isNew, isOld(clickLog.isNew, clickLog.isDayNew)),
          ChannelBrowser(clickLog.channelID.toString, clickLog.browserType, clickLog.yearMonthDayHour, clickLog.count, clickLog.isHourNew, clickLog.isNew, isOld(clickLog.isNew, clickLog.isHourNew))
        )
      }
    }
  }

  /**
   * 定义分组操作
   **/
  override def groupBy(mapDataStream: DataStream[ChannelBrowser]): KeyedStream[ChannelBrowser, String] = {
    mapDataStream.keyBy {
      item => item.channelID + ":" + item.browserType + ":" + item.date
    }
  }

  /**
   * 聚合操作实现
   **/
  override def reduce(windowStream: WindowedStream[ChannelBrowser, String, TimeWindow]): DataStream[ChannelBrowser] = {
    windowStream.reduce((priv, next) => ChannelBrowser(priv.channelID, priv.browserType, priv.date, priv.pv + next.pv, priv.uv + next.uv, priv.newCount + next.newCount, priv.oldCount + next.oldCount))
  }

  /**
   * 数据落地到hbase中
   **/
  override def sink2Hbase(reduceStream: DataStream[ChannelBrowser]): Unit = {
    //  数据入库保存到hbase数据集中
    reduceStream.addSink {
      value => {
        val rowKey = value.channelID + ":" + value.browserType + ":" + value.date
        val tableName = "channel_browser"
        val browserTypeColumn = "browserType"
        // 查询历史数据
        val mapData: Map[String, String] = HbaseUtil.getMapData(tableName, clfName, rowKey, List(pvColumn, uvColumn, newCountColumn, oldCountColumn))
        //  进行数据相加操作
        HbaseUtil.putMapData(tableName, clfName, rowKey, Map(
          channelIdColumn -> value.channelID,
          browserTypeColumn -> value.browserType,
          dateColumn -> value.date,
          pvColumn -> getTotal(mapData,pvColumn,value.pv),
          uvColumn -> getTotal(mapData,uvColumn,value.uv),
          newCountColumn -> getTotal(mapData,newCountColumn,value.newCount),
          oldCountColumn -> getTotal(mapData,oldCountColumn,value.oldCount)))
      }
    }
  }
}
