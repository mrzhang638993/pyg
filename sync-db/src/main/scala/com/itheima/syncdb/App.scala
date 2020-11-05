package com.itheima.syncdb

import com.itheima.syncdb.bean.Canal
import com.itheima.syncdb.util.FlinkUtil.init
import com.itheima.syncdb.util.FlinkUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

object App {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = init()
    val flinkKafkaConsumer: FlinkKafkaConsumer09[String] = FlinkUtil.initKafka(env)
    //增加数据源信息
    val kafkaDataStream: DataStream[String] = env.addSource(flinkKafkaConsumer)
    // kafka数据生成为一个canal的样例类对象执行操作
    val canalDataStream: DataStream[Canal] = kafkaDataStream.map(item => Canal(item))
    //println(canalDataStream)
    // 增加水印操作支持
    val waterValue: DataStream[Canal] = canalDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Canal] {
      var currentTimestamp:Long=0L
      val delayTime=2L
      // 增加水印支持操作
      override def getCurrentWatermark: Watermark = {
          new Watermark(currentTimestamp-delayTime)
      }

      // 增加水印操作支持
      override def extractTimestamp(element: Canal, previousElementTimestamp: Long): Long = {
        // 比较当前元素的时间和最大的时间，防止时间倒流才做
        currentTimestamp=Math.max(element.timestamp,previousElementTimestamp)
        currentTimestamp
      }
    })
    env.execute("sync-db")
  }
}
