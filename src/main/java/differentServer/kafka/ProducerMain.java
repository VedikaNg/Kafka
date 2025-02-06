package differentServer.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.Logger;

public class ProducerMain {

    public static void close(KafkaProducer kafkaProducer, Logger logger){
        logger.info("Flushing the information in Producer");
        kafkaProducer.flush();
        logger.info("Closing the Producer: "+kafkaProducer);
        kafkaProducer.close();
    }

    public static void sendToKafka(KafkaProducer kafkaProducer,Logger logger, String message, String topic){
        logger.info("Sending message from Producer: "+ kafkaProducer);
        kafkaProducer.send(new ProducerRecord<>(topic, message));
        logger.info("Message sent from the Producer");
    }

}
