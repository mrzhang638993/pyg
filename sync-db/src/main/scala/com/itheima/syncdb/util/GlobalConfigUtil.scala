package com.itheima.syncdb.util

import com.typesafe.config.{Config, ConfigFactory}

object  GlobalConfigUtil{
  // 通过工厂类加载配置文件操作.只能加载resources下面的配置信息的
   val   config:Config=ConfigFactory.load()
  //加载kafka配置操作实现
  val BOOTSTRAP_SERVERS: String = config.getString("bootstrap.servers")
   val ZOOKEEPER_CONNECT: String = config.getString("zookeeper.connect")
  val INPUT_TOPIC: String = config.getString("input.topic")
   val GROUP_ID: String = config.getString("group.id")
   val ENABLE_AUTO_COMMIT: String = config.getString("enable.auto.commit")
   val AUTO_COMMIT_INTERVAL_MS: String = config.getString("auto.commit.interval.ms")
   val AUTO_OFFSET_RESET: String = config.getString("auto.offset.reset")
}