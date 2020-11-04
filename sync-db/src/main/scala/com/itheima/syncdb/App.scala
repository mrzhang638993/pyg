package com.itheima.syncdb

import java.util.Properties

import com.itheima.syncdb.util.FlinkUtil.init
import com.itheima.syncdb.util.{FlinkUtil, GlobalConfigUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer09}

object App {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = init()
    val flinkKafkaConsumer: FlinkKafkaConsumer09[String] = FlinkUtil.initKafka(env)
    //增加数据源信息
    val kafkaDataStream: DataStream[String] = env.addSource(flinkKafkaConsumer)
    kafkaDataStream.print()
    env.execute("sync-db")
  }
}
