package com.itheima.batch.process.bean

/**
 * 支付方式角度进行数据统计操作实现
 * */
case class PaymethodMoneyCount(
                                var payMethod:String, // 支付方式
                                var date:String,      //  日期
                                var moneyCount:Double, //  订单金额
                                var count:Long         // 订单总数
                              )
