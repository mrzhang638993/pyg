# 离线分析操作实现
mysql中导入数据----->canal读取mysql的binlog日志，主从复制----->canal-kafka数据存入到kafka中
-------->sync-db同步kafka中的数据进行解析存储到hbase中的。------>Hbase中进行数据存储。


#
flink使用的hbase的版本是1.4.3的版本的，hbase—client对应的版本是2.0的版本的，存在不兼容的特性的。
需要修改相关的类的。

