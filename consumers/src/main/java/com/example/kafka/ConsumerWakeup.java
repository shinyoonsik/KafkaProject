package com.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerWakeup {

    public static void main(String[] args) {

        String topic = "test-test";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topic));

        Thread mainThread = Thread.currentThread();

        // addShutdownHook 메서드에 등록된 스레드는 JVM이 종료될 때 호출된다
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("starts to exit by calling wakeup!");
            kafkaConsumer.wakeup();

            try{
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try{
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord record : consumerRecords){
                    log.info("key: {} | value: {}", record.key(), record.value());

                    TopicPartition topicPartition = new TopicPartition(topic, 0);
                    OffsetAndMetadata committed = kafkaConsumer.committed(topicPartition);
                    System.out.println("커밋된 offset정보: " + committed.offset());

                }
            }
        } catch (Exception e){
            e.printStackTrace();
            log.error("WakeupException이 호출 됨");
        } finally {
            log.info("kafka Consumer 안전하게 종료!");
            kafkaConsumer.close();
        }
    }
}
