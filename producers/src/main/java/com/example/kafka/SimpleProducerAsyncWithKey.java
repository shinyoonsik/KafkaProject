package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducerAsyncWithKey {
    private static RecordMetadata .;

    public static void main(String[] args) {
        String topic = "multipart-topic";

        // KafkaProducer config setting
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 생성
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        // 특정 key -> 특정 partition
        // partition내에서만 순서가 보장된다
        for (int seq = 0; seq < 20; seq++) {
            // ProducerRecord 생성
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, seq, "hello world!");

            // 전송
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("partition:" + metadata.partition());
                    System.out.println("offset: " + metadata.offset());
                    System.out.println("timestamp: " + metadata.timestamp());
                    System.out.println();
                } else {
                    exception.printStackTrace();
                }
            });
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();

    }
}
