package com.itheima.batch.process.bean

import com.alibaba.fastjson.JSON

case class OrderRecord(
                        var benefitAmount:String, //红包金额
                        var orderAmount:String,  // 订单金额
                        var payAmount:String,    //  支付金额
                        var activityNum:String,  // 活动ID
                        var createTime:String,   // 创建时间
                        var merchantId:String,   //  商家ID
                        var orderId:String,      //  订单ID
                        var payTime:String,      // 支付时间
                        var payMethod:String,    //  支付方式
                        var voucherAmount:String, // 优惠券的金额
                        var commodityId:String,   //产品ID
                        var userId:String         // 用户ID
                      )
object OrderRecord{
  def apply(json:String): OrderRecord = {
     JSON.parseObject(json,classOf[OrderRecord])
  }
}