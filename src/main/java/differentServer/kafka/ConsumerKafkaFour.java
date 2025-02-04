package differentServer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaFour {
    private KafkaConsumer kafkaConsumer4;

    public void start(){
        Properties propertiesConsumer4 = new Properties();
        propertiesConsumer4.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9093");
        propertiesConsumer4.put(ConsumerConfig.GROUP_ID_CONFIG,"Group2");
        propertiesConsumer4.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        propertiesConsumer4.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer4 = new KafkaConsumer<>(propertiesConsumer4);
    }

    public void close(){
        kafkaConsumer4.close();
    }

    public void subscribe(){
        kafkaConsumer4.subscribe(Arrays.asList("topicA", "topicB"));
    }

    public void getMessage(){
        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer4.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                System.out.println(consumerRecord.key());
                System.out.println(consumerRecord.topic());
                System.out.println(consumerRecord.value());
                System.out.println(consumerRecord.offset());
            }
        }
    }

    public static void main(String[] args){
            ConsumerKafkaFour consumerKafkaFour = new ConsumerKafkaFour();
            consumerKafkaFour.start();
            consumerKafkaFour.subscribe();
            consumerKafkaFour.getMessage();
            consumerKafkaFour.close();
    }
}
