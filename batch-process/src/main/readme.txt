# 离线分析操作实现
mysql中导入数据----->canal读取mysql的binlog日志，主从复制----->canal-kafka数据存入到kafka中
-------->sync-db同步kafka中的数据进行解析存储到hbase中的。------>Hbase中进行数据存储。


#
flink使用的hbase的版本是1.4.3的版本的，hbase—client对应的版本是2.0的版本的，存在不兼容的特性的。
需要修改相关的类的。

# 修改框架的操作，根据继承关系，找到最上面的分支，重新书写对应的操作代码即可的。重新创建一个从上到下的分支即可实现的
下面是一个修改的样例类实现操作的
1.ItheimaTableInputFormat------>ItheimaAbstractTableInputFormat----->RichInputFormat(ItheimaTableInputSplit)
最终顶层的代码实现是依靠的是RichInputFormat从上到下的分支的。代码存在问题的话，只需要从上到下进行处理即可实现的。



ETL:预处理操作，就是将原始的字段进行拓宽处理操作。用户后续的其他业务的适用备用操作。
转换操作：将原始的字段转换成为业务分析需要的字段进行操作分析的。
分流:按照业务进行分组操作实现。实现分流处理实现。
聚合:对组内的数据进行聚合操作实现。进行数据的累加操作实现的。
落地:数据落地到hbase中进行操作操作实现的。


大数据的处理结果怎么进行实时的展示操作和实现管理的。


kafka-clients的最终版本是0.10.2.0的版本操作
