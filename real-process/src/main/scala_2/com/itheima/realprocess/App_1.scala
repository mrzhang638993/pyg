package com.itheima.realprocess

import com.alibaba.fastjson.JSON
import com.itheima.realprocess.bean._
import com.itheima.realprocess.task.{ChannelFreshnessTask1, ChannelPvUvTask1, ChannelRealHotTask1, PreProcessTask1}
import com.itheima.realprocess.util.{GlobalConfigUtil2, HbaseUtils1}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import java.lang
import java.util.Properties
import scala.collection.mutable
object App_1 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 并行度
    env.setParallelism(1)
    // 检查点机制，flink出现意外，重启之后怎么回复了？
    // flink基于operater的机制实现snapshot的快照机制实现操作
    // 5秒钟启动一次checkpoint操作机制
    env.enableCheckpointing(5000)
    // 设置checkpoint对应的只会执行一次操作。
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的时间间隔，间隔时间为1秒钟
    env.getCheckpointConfig.setCheckpointInterval(1000)
    // 设置checkpoint的超时时长，单次执行增量更新操作的时间不能超过60秒的时间。
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 设置checkpoint执行时候的并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 程序关闭的时候，触发checkpoint操作实现。
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的存储地址,存在的是一个多态的处理机制的。
    val backend:StateBackend = new FsStateBackend("hdfs://hadoop01:8020/flink-checkpoint/", false)
    env.setStateBackend(backend)
    // 整合kafka信息
    val topic=GlobalConfigUtil2.inputTopic
    val schema = new SimpleStringSchema()
    val props=new Properties() ;
    props.setProperty("bootstrap.servers",GlobalConfigUtil2.bootstrapServers)
    props.setProperty("zookeeper.connect",GlobalConfigUtil2.zookeeperConnect)
    props.setProperty("input.topic",GlobalConfigUtil2.inputTopic)
    props.setProperty("group.id",GlobalConfigUtil2.groupId)
    props.setProperty("enable.auto.commit",GlobalConfigUtil2.enableAutoCommit.toString)
    props.setProperty("auto.commit.interval.ms",GlobalConfigUtil2.autoCommitIntervalMs.toString)
    props.setProperty("auto.offset.reset",GlobalConfigUtil2.autoOffsetReset)
    val flinkConsumer = new FlinkKafkaConsumer010[String](topic,schema,props)
    val kafkaDataStream: DataStream[String] = env.addSource(flinkConsumer)
    val value: DataStream[Message1] = kafkaDataStream.map(msgJson => {
      val jsonObject = JSON.parseObject(msgJson)
      val message: String = jsonObject.getString("message")
      // 调用apply方法创造对应的样例类对象信息,对应的使用样例类对象执行操作实现和管理。
      val log: ClickLog1 = ClickLog1(message)
      val count: lang.Long = jsonObject.getLong("count")
      val timeStamp: lang.Long = jsonObject.getLong("timeStamp")
      Message1(log, count, timeStamp)
    })
    // flink的水印支持和相关的理解，watermark对应的是一个时间戳的概念的。
    // 避免因为网络的延迟,导致的数据计算的错误问题。增加水印时间的支持，保证数据的处理正常的操作和实现的。
    // 水印解决了网络延时,对应的消息没有被计算的问题的。网络延时特别高的话，
    // 对应的数据还是无法计算的。只能解决对应的时延范围内的数据的。
    val waterStream: DataStream[Message1] = value.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message1]() {
      var currentTimestamp: Long = 0L;
      // 定义水印的延迟时间
      val maxDelay = 2000L
      //获取当前的时间戳,对应的返回的是水印时间标记
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimestamp - maxDelay)
      }
      // 获取事件时间,事件时间是递增的,不能递减操作的。
      override def extractTimestamp(element: Message1, previousElementTimestamp: Long): Long = {
        currentTimestamp = Math.max(element.timeStamp, previousElementTimestamp)
        currentTimestamp
      }
    })
    val preTaskData: DataStream[ClickLogWide1] = PreProcessTask1.process(waterStream)
    val channelHotData: DataStream[ChannelRealHot1] = ChannelRealHotTask1.process(preTaskData)
    channelHotData.addSink(new SinkFunction[ChannelRealHot1]{
      override def invoke(value: ChannelRealHot1,context: SinkFunction.Context[_]): Unit = {
        // 从hbase中获取到历史的数据，执行累加的操作实现的。得到的是累加的数据结果和操作实现。
        val tableName:String="channel"
        val cfName:String="info"
        val rowKey:String=value.channelId
        val visitedColumn:String="visited"
        val originalData:String = HbaseUtils1.getData(tableName, rowKey, cfName, visitedColumn)
        var visited:Long=value.visited
        if(originalData!=null){
          visited+=originalData.toLong
        }
        HbaseUtils1.putData(tableName,rowKey,cfName,visitedColumn,visited.toString)
      }
    })
    val channelPvUvStream: DataStream[ChannelPvUv1] = ChannelPvUvTask1.process(preTaskData)
    channelPvUvStream.addSink(new SinkFunction[ChannelPvUv1] {
      override def invoke(value: ChannelPvUv1, context: SinkFunction.Context[_]): Unit ={
         // 将计算得到的渠道对应的pv以及uv的数据保存到hbase中
         val  tableNameStr:String="channel_pvuv"
         val  clfName:String="info"
         val  channelIdColumn:String="channelId"
         val  yearMonthDayHourColumn:String="yearMonthDayHour"
         val  pvColumn:String="pv"
         val  uvColumn:String="uv"
         val  rowKey:String=value.channelId+":"+value.yearDayMonthHour
         val pvUv: Map[String, String] = HbaseUtils1.getMapData(tableNameStr, rowKey, clfName, List(pvColumn, uvColumn))
         if (pvUv!=null){
           // 需要去除对应的字段为null的操作和相关的业务逻辑实现的,增加代码的健壮性。
           value.pv+=pvUv.get(pvColumn).get.toLong
           value.uv+=pvUv.get(uvColumn).get.toLong
         }
        HbaseUtils1.putMapData(tableNameStr,rowKey,clfName,mutable.Map(pvColumn->value.pv,uvColumn->value.uv,channelIdColumn->value.channelId,yearMonthDayHourColumn->value.yearDayMonthHour))
      }
    })
    // 将新老用户的数据写入到hbase中执行操作
    val freshAndOldUser: DataStream[ChannelFreshness1] = ChannelFreshnessTask1.process(preTaskData)
    freshAndOldUser.addSink(new SinkFunction[ChannelFreshness1] {
      override def invoke(value: ChannelFreshness1, context: SinkFunction.Context[_]): Unit ={
          val tableName:String="channel_freshness"
          val clfName:String="info"
          val rowkey=value.channelId+":"+value.date
          //  查询new历史数据
          val channelIdColumn:String="channelId"
          val dateColumn:String="date"
          val newCountColumn:String="newCount"
          val oldCountColumn:String="oldCount"
          val mapValue: Map[String, String] = HbaseUtils1.getMapData(tableName, rowkey, clfName, List(newCountColumn, oldCountColumn))
          if(mapValue!=null){
            val newCount:String = mapValue.getOrElse(newCountColumn, 0).toString
            val oldCount: String = mapValue.getOrElse(oldCountColumn, 0).toString
            HbaseUtils1.putMapData(tableName,rowkey,clfName,mutable.Map(newCountColumn->(value.newCount+newCount.toLong).toString,oldCountColumn->(value.oldCount+oldCount.toLong).toString,channelIdColumn->value.channelId,dateColumn->value.date))
          }else{
            HbaseUtils1.putMapData(tableName,rowkey,clfName,mutable.Map(newCountColumn->value.newCount.toString,oldCountColumn->value.oldCount.toString,channelIdColumn->value.channelId,dateColumn->value.date))
          }
      }
    })
    env.execute("real-process")
  }
}
