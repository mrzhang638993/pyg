package com.itheima.batch.process.task


import java.util.Properties

import com.itheima.batch.process.bean.Message
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaOutPut  extends  OutputFormat[Message]{

  private val bootstrapServer="192.168.1.201:9092,192.168.1.202:9092,192.168.1.203:9092"
  private val zookeeperConnect="node01:2181,node02:2181,node03:2181"
  private val inputTopic:String="exec1"
  private val groupId:String="exec1"
  private val enableAutoCommit:Boolean=true
  private val autoCommitIntervalMs:Boolean=true
  private val autoOffsetReset:String="latest"
  private var producer:KafkaProducer[String,Message]=null

  override def configure(parameters: Configuration): Unit = {

  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    val  properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServer)
    properties.put("acks", "all")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producer=new KafkaProducer(properties);
  }

  override def writeRecord(record: Message): Unit = {
    // 数据进行处理操作实现,发送数据进行操作
    producer.send(new ProducerRecord[String,Message](inputTopic,record))
  }

  override def close(): Unit = {
      if(producer!=null){
        producer.close()
      }
  }
}
