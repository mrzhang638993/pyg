package com.itheima.syncdb

import java.util.Properties

import com.itheima.syncdb.util.{FlinkUtil, GlobalConfigUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer09}

object App {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = FlinkUtil.init()
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/sync-db/"))
    // 整合构造kafka操作
    val properties = new Properties()
    properties.put("bootstrap.servers",GlobalConfigUtil.BOOTSTRAP_SERVERS)
    properties.put("group.id",GlobalConfigUtil.GROUP_ID)
    properties.put("enable.auto.commit",GlobalConfigUtil.ENABLE_AUTO_COMMIT)
    properties.put("auto.commit.interval.ms",GlobalConfigUtil.AUTO_COMMIT_INTERVAL_MS)
    properties.put("auto.offset.reset",GlobalConfigUtil.AUTO_OFFSET_RESET)
    val flinkKafkaConsumer = new FlinkKafkaConsumer010[String](
      GlobalConfigUtil.INPUT_TOPIC,
      new SimpleStringSchema(), properties
    )
    //增加数据源信息
    val kafkaDataStream: DataStream[String] = env.addSource(flinkKafkaConsumer)
    kafkaDataStream.print()
    env.execute("sync-db")
  }
}
