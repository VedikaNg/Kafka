package differentServer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaThree {
    private KafkaConsumer kafkaConsumer3;

    public void start(){
        Properties propertiesConsumer3 = new Properties();
        propertiesConsumer3.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        propertiesConsumer3.put(ConsumerConfig.GROUP_ID_CONFIG,"Group1");
        propertiesConsumer3.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        propertiesConsumer3.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer3 = new KafkaConsumer<>(propertiesConsumer3);
    }

    public void close(){
        kafkaConsumer3.close();
    }

    public void subscribe(){
        kafkaConsumer3.subscribe(Arrays.asList("topicB"));
    }

    public void getMessage(){
        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer3.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                System.out.println(consumerRecord.key());
                System.out.println(consumerRecord.topic());
                System.out.println(consumerRecord.value());
                System.out.println(consumerRecord.offset());
            }
        }
    }

    public static void main(String[] args){
        ConsumerKafkaThree consumerKafkaThree = new ConsumerKafkaThree();
        consumerKafkaThree.start();
        consumerKafkaThree.subscribe();
        consumerKafkaThree.getMessage();
        consumerKafkaThree.close();
    }
}
