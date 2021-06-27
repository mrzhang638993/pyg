package com.itheima.realprocess.util

import com.typesafe.config.{Config, ConfigFactory}

/**
 * 配置文件加载类
 * */
object GlobalConfigUtil2 {
   // 通过工厂类加载配置类信息
   val config: Config = ConfigFactory.load()
   val bootstrapServers: String = config.getString("bootstrap.servers")
   val zookeeperConnect: String = config.getString("zookeeper.connect")
   val inputTopic: String = config.getString("input.topic")
   val groupId: String = config.getString("group.id")
   val enableAutoCommit: Boolean = config.getBoolean("enable.auto.commit")
   val autoCommitIntervalMs: Long = config.getLong("auto.commit.interval.ms")
   val autoOffsetReset: String = config.getString("auto.offset.reset")

  def main(args: Array[String]): Unit = {
      println(bootstrapServers)
      println(zookeeperConnect)
      println(inputTopic)
      println(groupId)
      println(enableAutoCommit)
      println(autoCommitIntervalMs)
      println(autoOffsetReset)
  }
}
