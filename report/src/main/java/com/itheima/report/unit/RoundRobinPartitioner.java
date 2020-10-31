package com.itheima.report.unit;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义kafka的分区规则实现，实现消息均衡的发送到不同的分区中
 * */
public class RoundRobinPartitioner  implements Partitioner {
    //  使用并发包下面的线程安全的对象
    AtomicInteger count=new  AtomicInteger(0);
    //  返回分区号信息
    @Override
    public int partition(String topic, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //  获取分区的数量
        Integer partition = cluster.partitionCountForTopic(topic);
        int i = count.incrementAndGet();
        if (i>65535){
            count.set(0);
        }
        System.out.println(i%partition);
        return i%partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
