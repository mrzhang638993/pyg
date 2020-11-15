package com.itheima.report.controller;

import com.alibaba.fastjson.JSON;
import com.itheima.report.bean.Message;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@RestController
public class AdReceiver {

    @Resource
    private KafkaTemplate<String,String> kafkaTemplate;

    /**
     * 接受string类型的json数据
     * */
    @RequestMapping("/adReceive")
    public Map<String,String> receive(@RequestBody String json){
        //  构建message,发送message到kafka中进行操作
        Map map=new HashMap();
        Message message=new Message();
        message.setMessage(json);
        message.setCount(1);
        message.setTimestamp(System.currentTimeMillis());
        String msgInfo = JSON.toJSONString(message);
        try {
            kafkaTemplate.send("ad_test",msgInfo);
            map.put("success","true");
            return map;
        }catch (Exception e){
            map.put("success","false");
            return map;
        }
    }
}
