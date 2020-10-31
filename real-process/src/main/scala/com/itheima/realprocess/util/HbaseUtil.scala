package com.itheima.realprocess.util


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, Table, TableDescriptor, TableDescriptorBuilder}

/**
 * hbase操作的工具类对象
 *
 * 所有的代码操作对应的都是基于原有的api的基础上执行的操作的
 * */
object HbaseUtil {
  /**
   * hbase配置类,不需要指定配置。默认加载hbase-default.xml，hbase-site.xml
   * */
  var  conf: Configuration=HBaseConfiguration.create()
  /**
   * hbase连接
   * */
  var conn:Connection=ConnectionFactory.createConnection(conf);
  /**
   * hbase操作的api.获取admin的实例对象
   * */
   val admin: Admin = conn.getAdmin

   def  getTable(tableName:String,columnFamily:String): Unit ={
     val tableNames: TableName = TableName.valueOf(tableName.getBytes)
     val bool: Boolean = admin.tableExists(tableNames).booleanValue()
     if(!bool){
       val builder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNames)
       val family: ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes)
       val familyDesc: ColumnFamilyDescriptor = family.build()
       builder.setColumnFamily(familyDesc)
       admin.createTable(builder.build())
     }
     val table: Table = conn.getTable(tableNames)
     println(table)
   }

  def main(args: Array[String]): Unit = {
    getTable("test","info")
  }
}
