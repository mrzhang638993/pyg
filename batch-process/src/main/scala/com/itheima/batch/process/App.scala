package com.itheima.batch.process

import com.itheima.batch.process.util.HBaseTableInputFormat
import org.apache.flink.api.java.tuple
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
    val hbaseValue: DataSet[tuple.Tuple2[String, String]] = env.createInput(new HBaseTableInputFormat("mysql.pyg.orderRecord"))
    //  打印输出操作实现
    hbaseValue.print()
  }
}
