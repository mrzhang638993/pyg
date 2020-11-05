package com.itheima.syncdb.task

import java.util

import com.alibaba.fastjson.JSON
import com.itheima.syncdb.bean.{Canal, HbaseOperation, NameValuePair}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

import scala.collection.JavaConverters._
import scala.collection.mutable
object PreprocessTask {
    //flatMap对应的是一对多的操作实现
    def  process(waterValue: DataStream[Canal]):DataStream[HbaseOperation]={
        waterValue.flatMap{
          canal=>{
            // 将canal.columnValue转换成为scala中的集合操作实现
            val pairs: util.List[NameValuePair] = JSON.parseArray(canal.columnValueList, classOf[NameValuePair])
            val nameValueList: mutable.Buffer[NameValuePair] = pairs.asScala
            // 进行数据转换成操作
            var opType:String=canal.eventType
            var  tableName:String="mysql."+canal.dbName+"."+canal.tableName
            var clfName:String="info"
            var rowKey:String=nameValueList(0).columnValue
            // 便利集合数据，执行数据判断执行的insert还是update操作的。
            opType match {
              case "INSERT"=>nameValueList.map{
                 nameValue=>HbaseOperation(opType,tableName,clfName,rowKey,nameValue.columnName,nameValue.columnValue)
              }
              case "UPDATE"=>nameValueList.filter(_.isValid).map{
                nameValue=>HbaseOperation(opType,tableName,clfName,rowKey,nameValue.columnName,nameValue.columnValue)
              }
              case "DELETE"=>nameValueList.map{
                nameValue=>HbaseOperation(opType,tableName,clfName,rowKey,"","")
              }
            }
          }
        }
    }
}
