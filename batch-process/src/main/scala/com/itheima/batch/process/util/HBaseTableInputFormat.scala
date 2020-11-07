package com.itheima.batch.process.util

import com.alibaba.fastjson.JSONObject
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.util.Bytes

/**
 * 这是一个flink整合hbase的工具类信息，读取hbase中的数据。
 * hbase中的TableInputFormat，实现hbase的数据输入操作实现
 * Tuple2 中存放的是两个元素的，第一个是rowkey，另外一个是保存有列名称和列值的json串信息
 *
 * 实现的抽象方法：
 * 1.Scan 对应的返回的是scan对象的，
 * */
class HBaseTableInputFormat(var tableName:String)  extends  TableInputFormat[Tuple2[String,String]]{
  /**
   * 返回scan对象的
   * */
  override def getScanner: Scan = {
    scan=new Scan()
    scan
  }

  /**
   * 返回表的名称进行操作实现
   * */
  override def getTableName: String = {
    tableName
  }

  /**
   * 将结果集返回形成成为Tuple2对象的。
   * 转换hbase中获取到的result,进行转换成为Tuple2对象的。
   * */
  override def mapResultToTuple(result: Result): Tuple2[String,String] = {
      // 操作如下
      // 1. 获取rowkey数据信息
    val row: Array[Byte] = result.getRow
    val rowKey: String = Bytes.toString(row)
      // 2. 获取cell数组
    val cells: Array[Cell] = result.rawCells()
      // 3. 遍历数组，获取列名称和列的数值
      // 4. 构造jsonObject对象进行操作实现
    val jsonObject=new JSONObject()
    for (elem <- cells) {
      val cellValue: String = Bytes.toString(CellUtil.cloneValue(elem))
      val cellName: String = Bytes.toString(CellUtil.cloneQualifier(elem))
      jsonObject.put(cellName,cellValue)
    }
      // 5. 构造tuple对象进行操作。
    new Tuple2[String,String](rowKey,jsonObject.toString)
  }
}
