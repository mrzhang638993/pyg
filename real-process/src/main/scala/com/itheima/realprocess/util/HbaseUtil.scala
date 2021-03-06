package com.itheima.realprocess.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

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

   def  getTable(tableName:String,columnFamily:String): Table ={
     val tableNames: TableName = TableName.valueOf(tableName.getBytes)
     val bool: Boolean = admin.tableExists(tableNames).booleanValue()
     if(!bool){
       val builder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNames)
       val family: ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes)
       val familyDesc: ColumnFamilyDescriptor = family.build()
       builder.setColumnFamily(familyDesc)
       admin.createTable(builder.build())
     }
     conn.getTable(tableNames)
   }

  /**
   * tableName:表的名称
   * columnFamily:列族
   * rowKey: rowkey
   * columnFamily:列族
   * columnName：列明
   * columnValue:列的数值
   * */
  def   putData(tableName:String,columnFamily:String,rowKey:String,columnName:String,columnValue:String): Unit ={
    val table: Table = getTable(tableName, columnFamily)
    var put=new Put(rowKey.getBytes())
    // 对应的设置数据信息
    put.addColumn(columnFamily.getBytes(),columnName.getBytes(),columnValue.getBytes())
    // 保存数据
    try {
      table.put(put)
    }catch{
          /**
           * 出现异常关闭table对象
           * */
      case  e:Exception=> {
        println(e.getCause)
      }
    }finally {
      table.close()
    }
  }

  /**
   * 获取hbase数据
   * tableName：表名称
   * columnFamily:列族名称
   * rowKey: rowKey
   * columnName: 列名称
   * */
  def  getData(tableName:String,columnFamily:String,rowKey:String,columnName:String):String={
    val table: Table = getTable(tableName, columnFamily)
    val get=new Get(rowKey.getBytes)
    val result: Result = table.get(get)
    // 判断数据了结果是否为空
    try {
      if (result != null && result.containsColumn(columnFamily.getBytes, columnName.getBytes())) {
        val bytes: Array[Byte] = result.getValue(columnFamily.getBytes, columnName.getBytes)
        val str: String = Bytes.toString(bytes)
        str
      } else {
        ""
      }
    } catch {
      case  e:Exception=>{
        println(e.getCause)
        ""
      }
    } finally {
      table.close()
    }
  }
  // 存储多列数据的方法
  /**
   * tableName 表名称
   * columnFamily  列族
   * rowKey
   * map: 列名称以及对应的数据
   * */
  def putMapData(tableName:String,columnFamily:String,rowKey:String,map:Map[String,Any]): Unit ={
    val table: Table = getTable(tableName, columnFamily)
    val  put=new Put(rowKey.getBytes)
    map.map{
      it=>{
       put.addColumn(columnFamily.getBytes(),it._1.getBytes(),it._2.toString.getBytes())
      }
    }
    try {
      table.put(put)
    } catch {
      case  e:Exception=>println(e.getCause)
    } finally {
      table.close()
    }
  }

  // 批量获取多列数据的方法
  def getMapData(tableName:String,columnFamily:String,rowKey:String,map:List[String]):Map[String,String]={
    val table: Table = getTable(tableName, columnFamily)
    val get=new Get(rowKey.getBytes())
    val result: Result = table.get(get)
    try {
      val resultMap: Map[String, String] = map.map(col => {
        val value: Array[Byte] = result.getValue(columnFamily.getBytes(), col.getBytes())
        if (value != null && value.size > 0) {
          val strValue: String = Bytes.toString(value)
          col -> strValue
        } else {
          "" -> ""
        }
      }).filter(_._1 != "").toMap
      resultMap
    } catch {
      case  e:Exception=>{
        println(e.getCause)
        Map[String,String]()
      }
    } finally {
      table.close()
    }
  }
  /**
   * 执行hbase的delete操作实现
   * */
  def  deleteData(tableName:String,columnFamily:String,rowKey:String):Unit={
    val table: Table = getTable(tableName, columnFamily)
    val  delete=new Delete(rowKey.getBytes())
    try {
      table.delete(delete)
    } catch {
      case e:Exception => println(e)
    } finally {
      table.close()
    }
  }
  def main(args: Array[String]): Unit = {
    //getTable("test","info")
    //putData("test","info","1","t1","hello")
    //println(getData("test", "info", "1", "t1"))
    /* val map=Map(
      "t2"->"scala",
      "t3"->"hive",
      "t4"->"flink"
    )
    putMapData("test","info","1",map)*/
   /* val columnsValues: Map[String, String] = getMapData("test", "info", "1", List("t1", "t2", "t3", "t4"))
    println(columnsValues)*/
    //deleteData("test","info","1")
  }
}
