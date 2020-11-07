package com.itheima.batch.process.bean

case class MerchantCountTask(
                              var merchantId:String, // 支付方式
                              var date:String,      //  日期
                              var moneyCount:Double, //  订单金额
                              var count:Long         // 订单总数
                            )
