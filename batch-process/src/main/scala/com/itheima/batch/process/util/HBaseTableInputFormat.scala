package com.itheima.batch.process.util

import com.alibaba.fastjson.JSONObject
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes

/**
  * 这是Flink整合HBase的工具类,去读取HBase的表的数据
  *
  * 1. 继承TableInputFormat<T extends Tuple>, 需要指定泛型
  * 2. 泛型是Java的Tuple : org.apache.flink.api.java.tuple
  * 3. Tuple2 中第一个元素装载rowkey 第二个元素装 列名列值的JSON字符串
  * 4. 实现抽象方法
  * getScanner: 返回Scan对象, 父类中已经定义了scan ,我们在本方法中需要为父类的scan赋值
  * getTableName : 返回表名,我们可以在类的构造方法中传递表名
  * mapResultToTuple : 转换HBase中取到的Result进行转换为Tuple
  *
  *  a. 取rowkey
  *  b. 取cell数组
  *  c. 遍历数组,取列名和列值
  *  d. 构造JSONObject
  *  e. 构造Tuple2
  */
class HBaseTableInputFormat{

}
