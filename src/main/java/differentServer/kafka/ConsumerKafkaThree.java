package differentServer.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ConsumerKafkaThree {

    public static void main(String[] args){
        Logger logger = LogManager.getLogger(ConsumerKafkaThree.class);
        KafkaConsumer kafkaConsumer3=null;
        kafkaConsumer3 = ConfigurationProducerConsumer.configConsumer(kafkaConsumer3, "Group1","localhost:9092",logger);
        ConsumerMain.subscribe(logger, kafkaConsumer3,"topicB");
        ConsumerMain.getMessage(logger, kafkaConsumer3);
        logger.info("Stoping the Consumer 3");
        ConsumerMain.close(kafkaConsumer3);
    }
}
