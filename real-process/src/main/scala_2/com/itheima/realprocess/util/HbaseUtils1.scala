package com.itheima.realprocess.util

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import java.io.IOException
import scala.collection.mutable

object HbaseUtils1 {
  var conf: Configuration=null
  //存在IOException相关的信息
  var conn1: Connection = null
  //存在IOException需要继续操作处理和实现
  var admin2: Admin =null
  var table:Table=null

  def createConn(): Unit ={
    conf= HBaseConfiguration.create()
    try(conn1=ConnectionFactory.createConnection(conf),admin2=conn1.getAdmin)
    catch {
      case ex:IOException=>println(ex.getMessage)
    }
  }

  /**
   * 返回hbase下面的table对象信息,如果不存在则创建表数据信息。
   * */
  def  getTable(tableNameStr:String,columnFamilyName:String):Table={
    val str: TableName = TableName.valueOf(tableNameStr)
    if(!admin2.tableExists(TableName)){
      val descriptor: ColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyName.getBytes).build()
      val tableDesc: TableDescriptor = TableDescriptorBuilder.newBuilder(str).setColumnFamily(descriptor).build()
      try admin2.createTable(tableDesc)
      catch {
        case ex:IOException=>println(ex.getMessage)
      }
    }
    try table=conn1.getTable(str)
    catch {
      case ex:IOException=>println(ex.getMessage)
    }
    table
  }

  /**
   * 开发存储单列数据的方法
   * */
    def putData(tableNameStr:String,rowKey:String,columnFamily:String,columnName:String,columnValue:String)={
      val table: Table = getTable(tableNameStr, columnFamily)
      val put=new Put(rowKey.getBytes)
      put.addColumn(columnFamily.getBytes,columnName.getBytes, columnValue.getBytes)
      try table.put(put)
      catch {
        case ex:IOException=>println(ex.getMessage)
      }
    }

  /**
   * 获取hbase的数据信息
   * */
  def getData(tableNameStr:String,rowKey:String,columnFamily:String,columnName:String):String={
    val table: Table = getTable(tableNameStr, columnFamily)
    val get=new Get(rowKey.getBytes)
    try {
      val result: Result = table.get(get)
      if(result!=null&&result.containsColumn(columnFamily.getBytes,columnName.getBytes)){
        val bytes: Array[Byte] = result.getValue(columnFamily.getBytes, columnName.getBytes)
        Bytes.toString(bytes)
      }
    }catch {
      case ex:IOException=>println(ex.getMessage)
      table.close()
    }
    StringUtils.EMPTY
  }

  /**
   * 保存多个列的字段
   * */
  def  putMapData(tableNameStr:String,rowKey:String,columnFamily:String,propsAndValues:mutable.Map[String,String]): Unit ={
    val table1: Table = getTable(tableNameStr, columnFamily)
    val put=new Put(columnFamily.getBytes)
    for(keyValue <-propsAndValues){
      put.addColumn(columnFamily.getBytes(),keyValue._1.getBytes,keyValue._2.getBytes)
    }
    try table1.put(put)
    catch {
      case ex:IOException=>println(ex.getMessage)
    }
  }

  /**
   * 批量获取多列数据的方法
   * */
  def getMapData(tableNameStr:String,rowKey:String,columnFamily:String,columns: List[String]): Map[String,String] ={
    val table1: Table = getTable(tableNameStr, columnFamily)
    val get=new Get(rowKey.getBytes)
    try {
      val result: Result = table1.get(get)
      columns.map{
        column=>
          if(result.containsColumn(columnFamily.getBytes,column.getBytes)){
            val bytes: Array[Byte] = result.getValue(columnFamily.getBytes, column.getBytes)
            column->Bytes.toString(bytes)
          }else{
            column->StringUtils.EMPTY
          }
      }.filter(_._2!=StringUtils.EMPTY).toMap
    }catch {
      case ex:IOException=>println(ex.getMessage)
      table1.close()
      Map[String,String]()
    }
  }

  /**
   * 删除数据信息
   * */
    def deleteData(tableNameStr:String,rowKey:String,columnFamily:String): Unit ={
      val table1: Table = getTable(tableNameStr, columnFamily)
      val delete=new Delete(rowKey.getBytes())
      //需要查看一下是否会删除相关的table的连接的，这个是需要确认和操作的。
      // delete.addColumn() 可以删除单列的数据信息的。
      try table1.delete(delete)
      catch {
        case ex:IOException=>println(ex.getMessage)
      }
    }

  def main(args: Array[String]): Unit = {
    val table: Table = getTable("test", "info")
    println(table)
    //需要注意hbase中相关的rowkey的组成策略和相关的问题的实现。
    putData("test","1","info","t1","hello world")
    getData("test","1","info","t1")
    // 或者是使用如下的方式实现操作
    /*val propsAndValues=Map(
       "t2"->"scala",
       "t3"->"hive",
       "t4"->"flink"
    )*/
    val propsAndValues=new mutable.HashMap[String,String]()
    propsAndValues.put("t2","scala")
    propsAndValues.put("t3","hive")
    propsAndValues.put("t4","flink")
    putMapData("test","1","info",propsAndValues)
    val list = List("t1", "t2")
    getMapData("test","1","info",list)
  }
}
