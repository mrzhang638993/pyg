package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{AdClickLog, AdClickLogWide, ClickLogWide}
import com.itheima.realprocess.task.PreTask.isNewProcess
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

object PreprocessTask {

  // 定义预处理方法和相关的操作，主要进行字段的拓宽，增加字段is_new字段以及click_cnt字段
  def process(waterValue: DataStream[AdClickLog]): DataStream[AdClickLogWide] = {
    waterValue.map {
      msg => {
           // 增加is_new字段以及click_cnt字段
        val isNew: Int = anlayseIsNew(msg)
        var clickCnt:Int=0
        //  增加点击次数信息
        if(StringUtils.isBlank(msg.click_user_id)){
          clickCnt=0
        }
        // 构建拓宽之后的样例类对象
        new AdClickLogWide(
          msg.city,
          msg.ad_compaign,
          msg.ad_media,
          msg.ad_source,
          msg.corpurin,
          msg.device_type,
          msg.host,
          msg.t_id,
          msg.user_id,
          msg.click_user_id,
          msg.timestamp,
          isNew.longValue(),
          clickCnt.longValue()
        )
      }
    }
  }

  /**
   * 查询hbase判断hbase中是否存在数据
   * */
  def anlayseIsNew(adLog:AdClickLog):Int={
     val rowkey=adLog.user_id
     val tableName="user_history"
     val cfName="info"
     val userIdColumnName="user_id"
     val userId: String = HbaseUtil.getData(tableName, cfName, rowkey, userIdColumnName)
     if(StringUtils.isNotBlank(userId)){
       0
     }else{
       1
     }
  }
}
