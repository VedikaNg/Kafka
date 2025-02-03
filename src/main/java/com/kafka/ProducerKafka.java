package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerKafka {

    private KafkaProducer kafkaProducer;

    public void start(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<>(properties);

    }

    public void sendToKafka(String message, int i){
        kafkaProducer.send(new ProducerRecord<>("test-topic", Integer.toString(i), message));
    }

    public void close(){
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    public static void main(String[] args){
        ProducerKafka producerKafka = new ProducerKafka();
        producerKafka.start();

        for (int i = 0; i <5; i++) {
            String message = "Message " + i;
            producerKafka.sendToKafka(message, i);
//            System.out.println("Sent: " + message);
        }
        producerKafka.close();

    }

}
