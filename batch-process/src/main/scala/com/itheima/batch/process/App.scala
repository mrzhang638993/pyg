package com.itheima.batch.process

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object App {
  /**
   * 执行批处理操作实现
   * */
  def main(args: Array[String]): Unit = {
    //  获取批处理环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //  执行对应的操作实现
    env.setParallelism(1)
    //  测试输出
    val testValue: DataSet[String] = env.fromCollection(List("1", "2", "3"))
    testValue.print()
  }
}
