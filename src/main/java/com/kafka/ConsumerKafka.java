package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafka {
    private KafkaConsumer kafkaConsumer;

    public void start(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Demo");
        kafkaConsumer = new KafkaConsumer<>(properties);
    }

    public void subscribe(){
        kafkaConsumer.subscribe(Arrays.asList("test-topic"));
    }

    public void close(){
        kafkaConsumer.close();
    }

    public void getMessage(){
        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                System.out.println(consumerRecord.key());
                System.out.println(consumerRecord.topic());
                System.out.println(consumerRecord.value());
                System.out.println(consumerRecord.offset());
            }
        }
    }

    public static void main(String[] args){
        ConsumerKafka consumerKafka = new ConsumerKafka();
        consumerKafka.start();
        consumerKafka.subscribe();
        consumerKafka.getMessage();
        consumerKafka.close();

    }
}
