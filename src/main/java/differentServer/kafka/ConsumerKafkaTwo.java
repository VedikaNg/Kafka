package differentServer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaTwo {
    private KafkaConsumer kafkaConsumer2;

    public void start(){
        Properties propertiesConsumer2 = new Properties();
        propertiesConsumer2.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        propertiesConsumer2.put(ConsumerConfig.GROUP_ID_CONFIG,"Group1");
        propertiesConsumer2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        propertiesConsumer2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer2 = new KafkaConsumer<>(propertiesConsumer2);
    }

    public void close(){
        kafkaConsumer2.close();
    }

    public void subscribe(){
        kafkaConsumer2.subscribe(Arrays.asList("topicA"));
    }

    public void getMessage(){
        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer2.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                System.out.println(consumerRecord.key());
                System.out.println(consumerRecord.topic());
                System.out.println(consumerRecord.value());
                System.out.println(consumerRecord.offset());
            }
        }
    }

    public static void main(String[] args){
        ConsumerKafkaTwo consumerKafkaTwo = new ConsumerKafkaTwo();
        consumerKafkaTwo.start();
        consumerKafkaTwo.subscribe();
        consumerKafkaTwo.getMessage();
        consumerKafkaTwo.close();
    }

}
