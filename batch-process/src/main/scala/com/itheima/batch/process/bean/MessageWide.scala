package com.itheima.batch.process.bean

import java.util.TimeZone

import com.itheima.batch.process.task.PreprocessTask.formatTime
import org.apache.commons.lang3.time.FastDateFormat

case class MessageWide (
                        var adCampaign: String,
                        var adMedia: String,
                        var adSource: String,
                        var city: String,
                        var corpurin: String,
                        var deviceType: String,
                        var host: String,
                        var tId: Int,
                        var timestamp: String,
                        var userId: String,
                        var clickUserId: String,
                        var yearMonthDay: String, // 时间字段信息
                        var yearMonth: String, // 时间字段信息
                        var year: String // 时间字段信息
                      )

object MessageWide{
  def apply(message:Message): MessageWide = {
    new MessageWide(
      message.adCampaign,
      message.adMedia,
      message.adSource,
      message.city,
      message.corpurin,
      message.deviceType,
      message.host,
      message.tId,
      message.timestamp,
      message.userId,
      message.clickUserId,
      formatTime(message.timestamp, "yyyy-MM-dd"),
      formatTime(message.timestamp, "yyyy-MM"),
      formatTime(message.timestamp, "yyyy")
    )
  }
  def formatTime(time:String,format:String): String  ={
    // 原始的时间
    val sourceFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX")
    // 时间戳信息
    val time1: Long = sourceFormat.parse(time).getTime
    //  获取得到最终的时间戳信息
    val format1: FastDateFormat = FastDateFormat.getInstance(format)
    val str: String = format1.format(time1)
    str
  }
}