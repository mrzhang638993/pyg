package com.itheima.report.bean;

import java.io.Serializable;

/**
 * 消息的实体类对象
 * */
public class Message implements Serializable {

   //  需要消息次数
   private  int  count;

   // 消息的时间戳信息
   private long  timestamp;

   //  消息的消息体信息
   private String message;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Message{" +
                "count=" + count +
                ", timestamp=" + timestamp +
                ", message='" + message + '\'' +
                '}';
    }
}
