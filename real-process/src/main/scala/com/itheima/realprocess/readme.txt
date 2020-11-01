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

