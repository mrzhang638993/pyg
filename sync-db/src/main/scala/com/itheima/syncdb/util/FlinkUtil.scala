package com.itheima.syncdb.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

object FlinkUtil {

    def  init(): StreamExecutionEnvironment ={
      // 设置流式处理环境
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      //  设置并行度
      env.setParallelism(1)
      // 设置 checkpoint操作
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
      //  设置间隔2秒钟执行一次
      env.getCheckpointConfig.setCheckpointInterval(5000L)
      // 设置checkpoint超时5秒钟执行操作实现
      env.getCheckpointConfig.setCheckpointTimeout(6000L)
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000L)
      // 设置checkpoint的并行度操作实现
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      //  程序关闭的时候,对应的触发额外的checkpoint操作实现
      env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
      // 返回数据结果
      env
    }

  /**
   * 初始化kafka的执行环境信息
   * */
   def initKafka(env: StreamExecutionEnvironment): FlinkKafkaConsumer09[String] ={
     System.setProperty("HADOOP_USER_NAME", "root")
     env.setStateBackend(new FsStateBackend("hdfs://node01:8020/sync-db/"))
     // 整合构造kafka操作
     val properties = new Properties()
     properties.put("bootstrap.servers",GlobalConfigUtil.BOOTSTRAP_SERVERS)
     //properties.put("group.id",GlobalConfigUtil.GROUP_ID)
     properties.put("enable.auto.commit",GlobalConfigUtil.ENABLE_AUTO_COMMIT)
     properties.put("auto.commit.interval.ms",GlobalConfigUtil.AUTO_COMMIT_INTERVAL_MS)
     properties.put("auto.offset.reset",GlobalConfigUtil.AUTO_OFFSET_RESET)
     val flinkKafkaConsumer = new FlinkKafkaConsumer09[String](
       GlobalConfigUtil.INPUT_TOPIC,
       new SimpleStringSchema(), properties
     )
     flinkKafkaConsumer
   }
}
