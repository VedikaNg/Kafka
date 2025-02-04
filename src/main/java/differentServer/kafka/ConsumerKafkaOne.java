package differentServer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaOne {
    private KafkaConsumer kafkaConsumer1;

    public void start(){
        Properties propertiesConsumer1 = new Properties();
        propertiesConsumer1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        propertiesConsumer1.put(ConsumerConfig.GROUP_ID_CONFIG,"Group1");
        propertiesConsumer1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        propertiesConsumer1.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer1 = new KafkaConsumer<>(propertiesConsumer1);

    }

    public void close(){
        kafkaConsumer1.close();
    }

    public void subscribe(){
        kafkaConsumer1.subscribe(Arrays.asList("topicA"));
    }

    public void getMessage(){
        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer1.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                System.out.println("Topic Consumed by Consumer 1: " + consumerRecord.topic());
                System.out.println("Message consumed: "+consumerRecord.value());
            }
        }
    }

    public static void main(String[] args){
        ConsumerKafkaOne consumerKafkaOne = new ConsumerKafkaOne();
        consumerKafkaOne.start();
        consumerKafkaOne.subscribe();
        consumerKafkaOne.getMessage();
        consumerKafkaOne.close();

    }

}
