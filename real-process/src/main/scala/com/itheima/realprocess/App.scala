package com.itheima.realprocess

import java.lang
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.itheima.realprocess.bean.ClickLog
import com.itheima.realprocess.util.GlobalConfigUtil
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer09}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.scala._
/**
 * 初始化flink的流式环境
 * */
object App {

  def main(args: Array[String]): Unit = {
    // 获取流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置流的处理时间为eventTime的处理时间的.避免网络延时操作和实现机制操作的
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并行度操作
    env.setParallelism(1)
    //  设置flink的容错功能，checkpoint检查点操作。保存数据快照，缩短计算的时间操作管理
    //  设置5秒钟启动一次checkpoint操作实现
    env.enableCheckpointing(5000)
    // 设置checkpoint的模式为exactly_once操作的。
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //  设置最小时间间隔为1秒
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    //  设置checkpoint的超时时长，1分钟
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //  设置checkpoint的最大的并行度执行操作
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭的时候，触发额外的checkpoint操作的
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //  设置checkpoint的存储信息
    env.setStateBackend(new FsStateBackend("hdfs://cdh1:8020/flink-checkpoint/"))
    // 加载本地集合数据查看是否可以执行的
    val properties=new Properties();
    properties.put("bootstrap.servers",GlobalConfigUtil.BOOTSTRAP_SERVERS)
   properties.put("zookeeper.connect",GlobalConfigUtil.ZOOKEEPER_CONNECT)
    properties.put("input.topic",GlobalConfigUtil.INPUT_TOPIC)
    properties.put("group.id",GlobalConfigUtil.GROUP_ID)
   properties.put("enable.auto.commit",GlobalConfigUtil.ENABLE_AUTO_COMMIT)
    properties.put("auto.commit.interval.ms",GlobalConfigUtil.AUTO_COMMIT_INTERVAL_MS)
    properties.put("auto.offset.reset",GlobalConfigUtil.AUTO_OFFSET_RESET)
    val kafkaDataStream = new FlinkKafkaConsumer09[String](GlobalConfigUtil.INPUT_TOPIC, new SimpleStringSchema(), properties)
    val consumerDataStream: DataStream[String] = env.addSource(kafkaDataStream)
    //  处理json的数据
    val mapValue: DataStream[(ClickLog, lang.Long, lang.Long)] = consumerDataStream.map {
      item => {
        //  处理json解析操作
        val jsonObject: JSONObject = JSON.parseObject(item)
        val message: String = jsonObject.getString("message")
        val timeStamp: lang.Long = jsonObject.getLong("timestamp")
        val count: lang.Long = jsonObject.getLong("count")
        // 转化成为样例类对象
        (ClickLog(message), timeStamp, count)
      }
    }
    mapValue.print()
    env.execute("real-process")
  }
}
