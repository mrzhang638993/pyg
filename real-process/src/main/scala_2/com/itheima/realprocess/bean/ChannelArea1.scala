package com.itheima.realprocess.bean

/**
 * 渠道地域分析的样例类对象
 * */
case class ChannelArea1(
                         var channelId:String, //渠道id
                         var area:String,
                         var date:String,
                         var pv:Long,
                         var  uv:Long,
                         var newCount:Long,
                         var oldCount:Long
                       )
