# 离线分析操作实现
mysql中导入数据----->canal读取mysql的binlog日志，主从复制----->canal-kafka数据存入到kafka中
-------->sync-db同步kafka中的数据进行解析存储到hbase中的。------>Hbase中进行数据存储。

