package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
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
  def timeWindow(keyedStream:KeyedStream[T, String]):WindowedStream[T, String, TimeWindow]={
    keyedStream.timeWindow(Time.seconds(3))
  }

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

  /**
   * 检测老用户是否是第一次
   * */
  val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0

  /**
   * 相同的变量抽取出来.放到对应的模板信息中
   * */
  val clfName = "info"
  val channelIdColumn = "channelId"
  val dateColumn = "date"
  val newCountColumn = "newCount"
  val oldCountColumn = "oldCount"
  val pvColumn="pv"
  val uvColumn="uv"
  var totalPvCount=0L
  var totalUvCount=0L
  val networkColumn="network"
  val yearMonthDayHourColumn="yearMonthDayHour"
  val visitedColumn="visited"
  val areaColumn="area"
  var totalNewCount=0L
  var totalOldCount=0L


  /**
   * 获取totalValue信息
   * @param  resultMap
   * @Param  column
   * @Param  currentValue
   * @return  累计之后的数值
   **/
  def getTotal(resultMap: Map[String, String], column: String, currentValue: Long): Long = {
    var total = 0L
    if (resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(column, ""))) {
      total = resultMap(column).toLong + currentValue
    } else {
      total = currentValue
    }
    total
  }
}
