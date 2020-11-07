package com.itheima.batch.process.task

import com.itheima.batch.process.bean.{OrderRecordWide, PaymethodMoneyCount}
import com.itheima.batch.process.util.HBaseUtil
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

import scala.collection.immutable.HashMap

object PaymethodMoneyCountTask {
    /**
     * 从支付方式的角度进行数据的统计操作
     * */
    def process(orderRecord: DataSet[OrderRecordWide]):Unit={
       // 转换
        val methodValue: DataSet[PaymethodMoneyCount] = orderRecord.flatMap {
            orderData => {
                List(
                    PaymethodMoneyCount(orderData.payMethod, orderData.yearMonthDay, orderData.payAmount.toDouble, 1L),
                    PaymethodMoneyCount(orderData.payMethod, orderData.yearMonth, orderData.payAmount.toDouble, 1L),
                    PaymethodMoneyCount(orderData.payMethod, orderData.year, orderData.payAmount.toDouble, 1L)
                )
            }
        }
        // 分组
        val dateGroupValue: GroupedDataSet[PaymethodMoneyCount] = methodValue.groupBy(_.date)
        // 聚合
        val reduceValue: DataSet[PaymethodMoneyCount] = dateGroupValue.reduce((priv, next) => PaymethodMoneyCount(priv.payMethod, priv.date, priv.moneyCount + next.moneyCount, priv.count + next.count))
        // 落地到hbase中
      reduceValue.collect().foreach{
        content=>{
          //  获取rowkey的数据信息
          val tableName:String="analysis_payment"
          val clfName:String="info"
          val  rowKey:String=content.payMethod+":"+content.date
          val payMethodColumn:String="payMethod"
          val dateColumn:String="date"
          val moneyCountColumn:String="moneyCount"
          val countColumn:String="countColumn"
          HBaseUtil.putMapData(tableName,rowKey,clfName,Map(
            payMethodColumn->content.payMethod,
            dateColumn->content.date,
            moneyCountColumn->content.moneyCount,
            countColumn->content.count
          ))
        }
      }
    }
}
