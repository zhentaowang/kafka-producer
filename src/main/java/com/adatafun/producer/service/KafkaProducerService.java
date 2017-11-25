package com.adatafun.producer.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.*;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * KafkaProducerService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/11/15.
 */
@Service
public class KafkaProducerService {

    public String dataProducer(final JSONObject request) {

        try {
            Properties props = new Properties();
            InputStream in = new BufferedInputStream(new FileInputStream("src/main/resources/application.properties"));
            props.load(in);     ///加载属性列表

            Properties kafkaProp = new Properties();
            kafkaProp.put("bootstrap.servers", props.getProperty("kafka.bootstrap.servers"));
            kafkaProp.put("acks", props.getProperty("kafka.acks"));
            kafkaProp.put("retries", Integer.valueOf(props.getProperty("kafka.retries")));
            kafkaProp.put("batch.size", Integer.valueOf(props.getProperty("kafka.batch.size")));
            kafkaProp.put("linger.ms", Integer.valueOf(props.getProperty("kafka.linger.ms")));
            kafkaProp.put("buffer.memory", Integer.valueOf(props.getProperty("kafka.buffer.memory")));
            kafkaProp.put("key.serializer", props.getProperty("kafka.key.serializer"));
            kafkaProp.put("value.serializer", props.getProperty("kafka.value.serializer"));

            Producer<String, String> producer = new KafkaProducer<>(kafkaProp);
            int totalMessageCount = 10000;
            for (int i = 0; i < totalMessageCount; i++) {
                String value = String.format("%d,%s,%d", System.currentTimeMillis(), request.getString("airportCode")+i, 1300);
                System.out.println(value);
                producer.send(new ProducerRecord<>(request.getString("topic"), value), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.out.println("Failed to send message with exception " + exception);
                        }
                    }
                });
                Thread.sleep(1000L);
            }
            producer.close();
            return "成功";
        } catch (Exception e) {
            return  "失败";
        }

    }

}
