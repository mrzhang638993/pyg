package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ChannelPvUv, ChannelRealHot, ClickLogWide}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

trait BaseTask[T] {
   /**
    * 定义转换操作
    * */
   def map(clickLogWide: DataStream[ClickLogWide]):DataStream[T]

  /**
   * 定义分组操作
   * */
  def groupBy(mapDataStream:DataStream[T]):KeyedStream[T, String]

  /**
   * 定义时间窗口操作
   * */
  def timeWindow(keyedStream:KeyedStream[T, String]):WindowedStream[T, String, TimeWindow]

  /**
   * 聚合操作实现
   * */
  def  reduce(windowStream:WindowedStream[T, String, TimeWindow]): DataStream[T]

  /**
   * 数据落地到hbase中
   * */
  def  sink2Hbase(reduceStream:DataStream[T])

  /**
   * 定义最终的实现方式，定义执行的操作顺序和执行逻辑
   * 不允许子类进行重写代码操作
   * */
  def process(clickLogWide: DataStream[ClickLogWide]): Unit ={
    val mapDataStream: DataStream[T] = map(clickLogWide)
    val keyedStream: KeyedStream[T, String] = groupBy(mapDataStream)
    val windowStream: WindowedStream[T, String, TimeWindow] = timeWindow(keyedStream)
    val reduceStream: DataStream[T] = reduce(windowStream)
    sink2Hbase(reduceStream)
  }
}
