package com.itheima.batch.process.task

import com.itheima.batch.process.bean.{OrderRecord, OrderRecordWide}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

/**
 * 数据预处理操作实现
 * */
object PreprocessTask {
   /**
    * 执行数据预处理操作实现
    * */
   def process(orderRecord: DataSet[OrderRecord]):DataSet[OrderRecordWide]={
        //  执行数据的预处理操作实现
     orderRecord.map{
       orderRecordData=>OrderRecordWide(
         orderRecordData.benefitAmount,
         orderRecordData.orderAmount,
         orderRecordData.payAmount,
         orderRecordData.activityNum,
         orderRecordData.createTime,
         orderRecordData.merchantId,
         orderRecordData.orderId,
         orderRecordData.payTime,
         orderRecordData.payMethod,
         orderRecordData.voucherAmount,
         orderRecordData.commodityId,
         orderRecordData.userId,
         formatTime(orderRecordData.createTime,"yyyy-MM-dd"),
         formatTime(orderRecordData.createTime,"yyyy-MM"),
         formatTime(orderRecordData.createTime,"yyyy")
       )
     }
   }

  def formatTime(time:String,format:String): String  ={
    // 原始的时间
    val sourceFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    // 时间戳信息
    val time1: Long = sourceFormat.parse(time).getTime
    //  获取得到最终的时间戳信息
    val format1: FastDateFormat = FastDateFormat.getInstance(format)
    format1.format(time1)
  }
}
