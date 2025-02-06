package differentServer.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;

public class ConsumerMain {

    public static void close(KafkaConsumer kafkaConsumer){
        kafkaConsumer.close();
    }

    public static void subscribe(Logger logger, KafkaConsumer kafkaConsumer, String topic){
        logger.info("Subscribing to the topic");
        kafkaConsumer.subscribe(Arrays.asList(topic));
        logger.info("Subscribed to the topic: " + topic);
    }

    public static void getMessage(Logger logger, KafkaConsumer kafkaConsumer){
        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                logger.info("Topic Consumed by "+kafkaConsumer+ ": " + consumerRecord.topic()+ "Message consumed: "+consumerRecord.value());
            }
        }
    }

}
