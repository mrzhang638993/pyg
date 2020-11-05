package com.itheima.syncdb.bean

/**
 *
 *
 * */
case class HbaseOperation(
                         var opType:String,
                         var  tableName:String,
                         var clfName:String,
                         var rowKey:String,
                         var colName:String,
                         var  colValue:String
                         )
