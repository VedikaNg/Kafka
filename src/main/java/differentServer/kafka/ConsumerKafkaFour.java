package differentServer.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConsumerKafkaFour {

    public static void main(String[] args){
        KafkaConsumer kafkaConsumer4=null;
        Logger logger = LogManager.getLogger(ConsumerKafkaFour.class);
        kafkaConsumer4 = ConfigurationProducerConsumer.configConsumer(kafkaConsumer4,"Group2","localhost:9093, localhost:9092",logger);
        ConsumerMain.subscribe(logger, kafkaConsumer4, "topicA ,topicB");
        ConsumerMain.getMessage(logger, kafkaConsumer4);
        logger.info("Stoping the Consumer 4");
        ConsumerMain.close(kafkaConsumer4);
    }
}
