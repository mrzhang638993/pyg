package com.itheima.realprocess.bean

import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * 构建退款之后的样例类对象
 * */
case class AdClickLogWide(
                           city:String,
                           ad_compaign:String,
                           ad_media:String,
                           ad_source:String,
                           corpurin:String,
                           device_type:String,
                           host:String,
                           t_id:String,
                           user_id:String,
                           click_user_id:String,
                           timestamp:String,
                           isNew:Long,
                           clickCnt:Long
                         )
