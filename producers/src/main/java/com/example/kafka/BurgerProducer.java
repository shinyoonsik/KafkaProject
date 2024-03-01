package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class BurgerProducer {
    public static void main(String[] args) throws InterruptedException {
        String topic = "burger-topic";

        // KafkaProducer config setting
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // 메시지 전송
        int sum = 0;
        for(int i=0; i<10; i++){
            long before = System.currentTimeMillis();
            sendBurgerMessage(kafkaProducer, topic, 100000, true);
            long after = System.currentTimeMillis();

            System.out.println("Processing Time: " + (after - before) + "ms");
            sum += (after - before);
        }
        System.out.println("최종결과: " + sum / 5 + "ms");

        kafkaProducer.close();
    }

    /**
     * @param kafkaProducer
     * @param topic
     * @param reps  메시지를 전송하는 횟수. -1은 무한 반복
     */
    public static void sendBurgerMessage(KafkaProducer<String, String> kafkaProducer, String topic, int reps, boolean sync) throws InterruptedException {
        BurgerMessage burgerMessage = new BurgerMessage();

        int seq = 0;
        long seed = 123L;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (seq++ != reps) {
            Map<String, String> message = burgerMessage.getMessage(faker, random, seq);
            // key가 있는 전송 => 특정 key -> 특정 partition
            // ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message.get("key"), message.get("message"));

            // key가 없는 전송
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,  message.get("message"));

            // Thread.sleep(50);
            sendMessage(kafkaProducer, producerRecord, message, sync);
        }
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                    ProducerRecord<String, String> producerRecord, Map<String, String> message, boolean sync) {
        if (sync) {
            try {
                RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            //  System.out.println("토픽:" + recordMetadata.topic() + " | 파티션:" + recordMetadata.partition() + " | 오프셋: " + recordMetadata.offset() + " 메시지Key: " + message.get("key"));
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            kafkaProducer.send(producerRecord, ((recordMetadata, exception) -> {
                if (exception == null) {
                //  System.out.println("토픽:" + recordMetadata.topic() + " | 파티션:" + recordMetadata.partition() + " | 오프셋: " + recordMetadata.offset() + "메시지Key: " + message.get("key"));
                } else {
                    exception.printStackTrace();
                }
            }));
        }
    }


}
