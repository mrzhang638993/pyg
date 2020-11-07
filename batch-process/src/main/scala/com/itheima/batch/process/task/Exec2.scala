package com.itheima.batch.process.task

import java.io.{File, FileOutputStream, OutputStream}
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.itheima.batch.process.bean.{Message, MessageWide}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}



/**
 * 从kafka中读取数据
 * */
object Exec2 {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    //  获取kafka的流式操作环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置检查点操作
    // 设置流的处理时间为eventTime的处理时间的.避免网络延时操作和实现机制操作的
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并行度操作
    env.setParallelism(1)
    //  设置flink的容错功能，checkpoint检查点操作。保存数据快照，缩短计算的时间操作管理
    //  设置5秒钟启动一次checkpoint操作实现
    env.enableCheckpointing(1200000L)
    // 设置checkpoint的模式为exactly_once操作的。
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //  设置最小时间间隔为1秒
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    //  设置checkpoint的超时时长，1分钟
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //  设置checkpoint的最大的并行度执行操作
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭的时候，触发额外的checkpoint操作的
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //  设置checkpoint的存储信息
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/exec1/"))
    val properties=new Properties();
    properties.put("bootstrap.servers","node01:9092,node02:9092,node03:9092")
    properties.put("input.topic","exec1")
    properties.put("group.id","exec1")
    properties.put("enable.auto.commit","true")
    properties.put("auto.commit.interval.ms","5000")
    properties.put("auto.offset.reset","latest")
    val kafkaDataStream = new FlinkKafkaConsumer010[String]("exec1", new SimpleStringSchema(), properties)
    val consumerDataStream: DataStream[String] = env.addSource(kafkaDataStream)
    val messageValue: DataStream[Message] = consumerDataStream.map {
      item => getMessage(item)
    }
    //下面开始相关的数据操作实现
    val messageAddValue: DataStream[MessageWide] = messageValue.map {
      message => {
        val wide: MessageWide = MessageWide(message)
        wide
      }
    }
    messageAddValue.addSink(new MySinkFunction())
    env.execute("exec2")
  }

  def  getMessage(json:String):Message={
    val message: Message = JSON.parseObject(json, classOf[Message])
    message
  }
}

class MySinkFunction extends  RichSinkFunction[MessageWide] with Serializable {
  private  val fileName="E:\\idea_works\\pyg\\batch-process\\src\\main\\scala\\com\\itheima\\batch\\process\\task\\result.log"

  private  var file:File=new File(fileName)
  private var fos:FileOutputStream=null

  override def invoke(value: MessageWide): Unit = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val str: String =  write(value)
    fos=new FileOutputStream(file,true)
    fos.write(str.getBytes())
    fos.write("\n".getBytes())
    fos.flush()
    fos.close()
  }
}


