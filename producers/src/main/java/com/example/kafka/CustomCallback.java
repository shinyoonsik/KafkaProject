package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CustomCallback implements Callback {

    private int key;

    public CustomCallback(int key) {
        this.key = key;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            System.out.println("messageKey: " + this.key);
            System.out.println("partition:" + metadata.partition());
            System.out.println("offset: " + metadata.offset());
            System.out.println("timestamp: " + metadata.timestamp());
            System.out.println();
        } else {
            exception.printStackTrace();
        }
    }
}
