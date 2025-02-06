package differentServer.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Scanner;

public class ProducerKafkaOne {

    public static void main(String[] args){
        KafkaProducer kafkaProducer1=null;
        Logger logger = LogManager.getLogger(ProducerKafkaOne.class);
        kafkaProducer1 = ConfigurationProducerConsumer.configProducer(kafkaProducer1, logger,"localhost:9092");
        Scanner scanner = new Scanner(System.in);
        String message;
        int count =10;
        while(count>0){
            logger.info("Asking to type the message");
            System.out.print("Enter the message: ");
            message = scanner.nextLine();
            logger.info("Sending to kafka");
            ProducerMain.sendToKafka(kafkaProducer1, logger, message, "topicA");
            logger.info("Sent to kafka");
            count--;
        }
        ProducerMain.close(kafkaProducer1, logger);
    }
}
