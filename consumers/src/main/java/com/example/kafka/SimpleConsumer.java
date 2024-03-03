package com.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class SimpleConsumer {

    public static void main(String[] args) {

        String topic = "simple-topic4";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");
//        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topic));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord record : consumerRecords){
                log.info("key: {} | value: {}", record.key(), record.value());

                TopicPartition topicPartition = new TopicPartition(topic, 0);
                OffsetAndMetadata committed = kafkaConsumer.committed(topicPartition);
                System.out.println("커밋된 offset정보: " + committed.offset());

            }


        }


    }
}
