package com.itheima.batch.process.task

import com.itheima.batch.process.bean.OrderRecordWide
import org.apache.flink.api.scala.DataSet

/**
 * 统计商家年，月，日维度的进行订单数量和支付金额的统计操作实现
 * */
object MerchantCountTaskMoney {

    def process(orderRecord: DataSet[OrderRecordWide]):Unit={

    }
}
