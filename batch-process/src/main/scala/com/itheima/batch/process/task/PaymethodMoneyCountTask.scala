package com.itheima.batch.process.task

import com.itheima.batch.process.bean.{ OrderRecordWide, PaymethodMoneyCount}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

object PaymethodMoneyCountTask {
    /**
     * 从支付方式的角度进行数据的统计操作
     * */
    def process(orderRecord: DataSet[OrderRecordWide]):DataSet[PaymethodMoneyCount]={
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
        val tableName:String=""
        val clfName=""

        //HBaseUtil.putMapData(tableName,clfName,)
        null
    }
}
