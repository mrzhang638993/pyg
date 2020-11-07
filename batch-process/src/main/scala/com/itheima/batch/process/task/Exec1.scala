package com.itheima.batch.process.task

import com.itheima.batch.process.bean.Message
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
/**
 * 执行数据操作实现
 * */
object Exec1 {

  def main(args: Array[String]): Unit = {
      // 数据存储到kafka中
      val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
    val sourceDataSet: DataSet[String] = env.readTextFile("E:\\idea_works\\pyg\\batch-process\\src\\main\\scala\\com\\itheima\\batch\\process\\task\\ad.log")
    val batchValue: DataSet[Message] = sourceDataSet.map {
      item => Message(item)
    }
    println(123)
    //batchValue.print()
  }
}
