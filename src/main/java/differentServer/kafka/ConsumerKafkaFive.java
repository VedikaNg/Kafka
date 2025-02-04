package differentServer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaFive {
    private KafkaConsumer kafkaConsumer5;

    public void start(){
        Properties propertiesConsumer5 = new Properties();
        propertiesConsumer5.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9093");
        propertiesConsumer5.put(ConsumerConfig.GROUP_ID_CONFIG,"Group2");
        propertiesConsumer5.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        propertiesConsumer5.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer5 = new KafkaConsumer<>(propertiesConsumer5);
    }

    public void close(){
        kafkaConsumer5.close();
    }

    public void subscribe(){
        kafkaConsumer5.subscribe(Arrays.asList("topicB"));
    }

    public void getMessage(){
        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer5.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                System.out.println(consumerRecord.key());
                System.out.println(consumerRecord.topic());
                System.out.println(consumerRecord.value());
                System.out.println(consumerRecord.offset());
            }
        }
    }

    public static void main(String[] args){
        ConsumerKafkaFive consumerKafkaFive = new ConsumerKafkaFive();
        consumerKafkaFive.start();
        consumerKafkaFive.subscribe();
        consumerKafkaFive.getMessage();
        consumerKafkaFive.close();
    }
}
