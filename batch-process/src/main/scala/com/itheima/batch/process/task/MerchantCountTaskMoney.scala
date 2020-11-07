package com.itheima.batch.process.task

import com.itheima.batch.process.bean.{MerchantCountTask, OrderRecordWide}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

/**
 * 统计商家年，月，日维度的进行订单数量和支付金额的统计操作实现
 * */
object MerchantCountTaskMoney {

    def process(orderRecord: DataSet[OrderRecordWide]):Unit={
      //  执行转换操作实现
      val merchantTask: DataSet[MerchantCountTask] = orderRecord.flatMap {
        orderContent => {
          List(
            MerchantCountTask(orderContent.merchantId, orderContent.yearMonthDay, orderContent.payAmount.toDouble, 1),
            MerchantCountTask(orderContent.merchantId, orderContent.yearMonth, orderContent.payAmount.toDouble, 1),
            MerchantCountTask(orderContent.merchantId, orderContent.year, orderContent.payAmount.toDouble, 1)
          )
        }
      }
      // 执行分组聚合操作实现
      val groupValue: GroupedDataSet[MerchantCountTask] = merchantTask.groupBy(_.date)
      //  执行reduce操作实现
      val reduceValue: DataSet[MerchantCountTask] = groupValue.reduce((priv, next) => MerchantCountTask(priv.merchantId, priv.date, priv.moneyCount + next.moneyCount, priv.count + next.count))
      // 数据写入到hbase中进行操作实现
      reduceValue.collect().foreach{
         reduces=>{
           val  tableName:String=""
           val  ctfName:String=""
         }
      }
    }
}
