package com.itheima.report;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
@RunWith(SpringRunner.class)
public class KafkaTemplateTests {


    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Test
    public void sendMsg(){
        for(int i=0;i<100;i++){
            kafkaTemplate.send("test","key","this is a msg");
        }
    }
}
