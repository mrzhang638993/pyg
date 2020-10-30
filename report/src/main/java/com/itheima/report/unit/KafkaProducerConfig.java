package com.itheima.report.unit;

import com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap_servers_config}")
    private  String bootstrapServersConfig;

    @Value("${kafka.retries_config}")
    private  String retriesConfig;

    @Value("${kafka.batch_size_config}")
    private  String batchSizeConfig;

    @Value("${kafka.linger_ms_config}")
    private  String lingerMsConfig;

   @Value("${kafka.buffer_memory_config}")
    private  String bufferMemoryConfig;

    @Value("${kafka.topic}")
   private  String topic;

    @Bean
    public KafkaTemplate<String,String> kafkaTemplate(){
        Map<String, Object> configs=new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServersConfig);
        configs.put(ProducerConfig.RETRIES_CONFIG,retriesConfig);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG,batchSizeConfig);
        configs.put(ProducerConfig.LINGER_MS_CONFIG,lingerMsConfig);
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG,bufferMemoryConfig);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String,String>  producerFactory= new DefaultKafkaProducerFactory<>(configs);
        KafkaTemplate<String,String> kafkaTemplate=new KafkaTemplate<>(producerFactory);
        return kafkaTemplate;
    }
}
