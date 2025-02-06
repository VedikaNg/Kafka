package differentServer.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConsumerKafkaFive {

    public static void main(String[] args){
        KafkaConsumer kafkaConsumer5=null;
        Logger logger = LogManager.getLogger(ConsumerKafkaFive.class);
        kafkaConsumer5 = ConfigurationProducerConsumer.configConsumer(kafkaConsumer5,"Group2","localhost:9093", logger);
        ConsumerMain.subscribe(logger, kafkaConsumer5, "topicB");
        ConsumerMain.getMessage(logger, kafkaConsumer5);
        logger.info("Stopping Consumer 5");
        ConsumerMain.close(kafkaConsumer5);
    }
}
