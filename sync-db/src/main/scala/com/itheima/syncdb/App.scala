package com.itheima.syncdb

import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object App {
  def main(args: Array[String]): Unit = {
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
    // 执行相关的操作机制实现
    env.setStateBackend(new FsStateBackend("hdfs://cdh1:8020/sync-db"))
  }
}
