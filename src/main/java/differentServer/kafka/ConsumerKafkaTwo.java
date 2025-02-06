package differentServer.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConsumerKafkaTwo {
    public static void main(String[] args){
        KafkaConsumer kafkaConsumer2 = null;
        Logger logger = LogManager.getLogger(ConsumerKafkaTwo.class);
        kafkaConsumer2 = ConfigurationProducerConsumer.configConsumer(kafkaConsumer2,"Group1","localhost:9092, localhost:9093" , logger);
        ConsumerMain.subscribe(logger, kafkaConsumer2, "topicA");
        ConsumerMain.getMessage(logger, kafkaConsumer2);
        logger.info("Stoping the Consumer 2");
        ConsumerMain.close(kafkaConsumer2);
    }

}
