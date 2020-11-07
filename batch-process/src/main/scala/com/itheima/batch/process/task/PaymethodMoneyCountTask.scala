package com.itheima.batch.process.task

import com.itheima.batch.process.bean.OrderRecord
import org.apache.flink.api.scala.DataSet

object PaymethodMoneyCountTask {
    /**
     * 从支付方式的角度进行数据的统计操作
     * */
    def process(orderRecord: DataSet[OrderRecord]):DataSet[]={

    }
}
