# 如下的代码存在疑问信息？
 val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
# 如下的代码存在疑问？
总的时间是大于的，获取不同类型的字符串的话，得到的结果不一样的？
isDayNew= compareTime(msg.timeStamp, lastVisitedTime.toLong, "yyyyMMdd")
isHourNew= compareTime(msg.timeStamp, lastVisitedTime.toLong, "yyyyMMddHH")
isHourNew= compareTime(msg.timeStamp, lastVisitedTime.toLong, "yyyyMM")

# 采集系统不要使用sql语句的，使用sql语句执行操作的话，会加大数据库的执行压力和相关的操作实践的。
在mysql中执行sql语句的话，会增大数据库的执行压力的。不推荐使用sql语句操作数据库的。
可以使用mysql的binlog手动开启操作实现的。
canel使用mysql的binlog执行操作的。避免了直接执行mysql的sql语句导致的性能损耗问题和实现的。
在mysql的主从复制操作中，canel会伪装成为mysql的从节点，从而获取到mysql的binlog日志文件的。

mysql使用binlog的话，需要手动的配置和启动binlog日志信息的。
binlog文件：二进制文件,记录了增删改命令，需要手动开启binlog日志功能和实现操作。
canel会解析数据库的binlog日志文件。
需要确定druib的组织结构数据和操作实现的。所有的操作节点对应的都是overload操作实现的。
web系统中如何调用druid以及从druid中获取到数据的。
druid使用web-api使用rest方式实现查询操作即可的。使用rest方式获取到对应的数据给前端展示即可进行操作的。
hbase也是可以执行web的rest查询操作的。实时的查询可以使用druid以及hbase实现实时查询操作实现。
对于明细数据的要求不高的话，可以使用druib进行实时数据的统计操作实现的。明细数据不是很详细的要求的。目前的druid还是使用json的操作的
还要求需要很复杂的json处理操作的。等到sql出现的时候可以调用sql的方式实现更加好的交互方式操作的。



