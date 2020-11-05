package com.itheima.syncdb.bean

import com.alibaba.fastjson.JSON

case class Canal(
                  emptyCount:Int,
                  logFileName:String,
                  dbName:String,
                  logFileOffset:Int,
                  eventType:String,
                  columnValueList:String,
                  tableName:String,
                  timestamp:Long
                )
object Canal{
  def apply( json:String): Canal ={
    //  scala 中的语法判断操作 classOf
    JSON.parseObject[Canal](json, classOf[Canal])
  }
}
