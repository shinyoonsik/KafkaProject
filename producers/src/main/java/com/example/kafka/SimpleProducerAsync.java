package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class SimpleProducerAsync {
    public static void main(String[] args) {
        String topic = "simple-topic";

        // KafkaProducer config setting
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord 생성
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello world!");

        // send()가 된다고 바로 record를 전송하지 않는다
        // onCompletion()을 실제로 수행하는 스레드는 sender 스레드
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println(metadata.partition());
                System.out.println(metadata.offset());
                System.out.println(metadata.timestamp());
            } else {
                exception.printStackTrace();
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();

    }
}
