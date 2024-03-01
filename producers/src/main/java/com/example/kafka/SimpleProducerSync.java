package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {
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
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(record).get();
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println(recordMetadata.timestamp());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            kafkaProducer.close();
        }
    }
}
