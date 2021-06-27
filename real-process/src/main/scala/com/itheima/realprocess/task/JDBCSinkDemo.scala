package com.itheima.realprocess.task

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.sql.{Connection, PreparedStatement}

object JDBCSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //准备数据源
    val tuples = List(("跑鞋", "耐克", 998), ("短裤", "李宁", 119), ("袜子", "耐克", 68), ("西服", "海澜之家", 1000))
    val dataValue: DataStream[(String, String, Int)] = env.fromCollection(tuples)

    dataValue.addSink(new RichSinkFunction[(String, String, Int)] {
      val conn:Connection=null
      val ps:PreparedStatement=null
      // 做全局的配置操作和实现管理
      override def open(conf: Configuration): Unit = {
        super.open(conf)
        val  url = "jdbc:mysql://localhost:3306/flink_data?useUnicode=true&characterEncoding=utf-8&serverTimezone=GMT";
        val  sql = "insert into t_product(name, brand, price) values (?,?,?)";
        Class.forName("")
        //DriverManager.registerDriver()
      }
      override def invoke(value: (String, String, Int), context: SinkFunction.Context[_]): Unit ={

      }
      override def close(): Unit ={
        // 执行资源关闭的方式和方法
      }
    })
    env.execute("jdbc data sink");
  }
}
