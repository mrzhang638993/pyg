package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide1
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

// 定义模板设计方式实现操作的。使用模板设计模式重构相关的设计代码和实现。
trait BaseTask1[T] {

  def map(clickLogWide:DataStream[ClickLogWide1]):DataStream[T]

  def groupBy(value:DataStream[T]):KeyedStream[T,String]

  // 抽取通用的实现方式的代码,在特质中的默认实现方式的。
  def timeWindow(value: KeyedStream[T,String]):WindowedStream[T, String, TimeWindow]={
    //  特质中可以提供方法的默认实现
      value.timeWindow(Time.seconds(3))
  }

  def  reduce(value: WindowedStream[T,String, TimeWindow]):DataStream[T]

  def sinkToHbase(value:DataStream[T])

  def process(clickLogWide:DataStream[ClickLogWide1]): Unit ={
    val value: DataStream[T] = map(clickLogWide)
    val value1: KeyedStream[T, String] = groupBy(value)
    val value2: WindowedStream[T, String, TimeWindow] = timeWindow(value1)
    val value3: DataStream[T] = reduce(value2)
    sinkToHbase(value3)
  }
}
