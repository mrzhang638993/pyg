package com.itheima.batch.process

import com.itheima.batch.process.bean.{OrderRecord, OrderRecordWide}
import com.itheima.batch.process.task.PreprocessTask
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
    val orderRecord: DataSet[OrderRecord] = hbaseValue.map {
      item => {
        OrderRecord(item.f1)
      }
    }
    // 进行数据的预处理操作实现
    val wideDataSet: DataSet[OrderRecordWide] = PreprocessTask.process(orderRecord)
    wideDataSet.print()
  }
}
