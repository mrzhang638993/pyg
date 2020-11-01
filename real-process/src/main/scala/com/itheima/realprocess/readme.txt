# 如下的代码存在疑问信息？
 val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
# 如下的代码存在疑问？
总的时间是大于的，获取不同类型的字符串的话，得到的结果不一样的？
isDayNew= compareTime(msg.timeStamp, lastVisitedTime.toLong, "yyyyMMdd")
isHourNew= compareTime(msg.timeStamp, lastVisitedTime.toLong, "yyyyMMddHH")
isHourNew= compareTime(msg.timeStamp, lastVisitedTime.toLong, "yyyyMM")