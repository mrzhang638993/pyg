package com.itheima.realprocess.bean

case class ChannelFreshness1(
                           var channelId:String, //渠道id
                           var date:String,   //日期
                           var newCount:Long, //新用户
                           var oldCount:Long  //老用户
                           )
