package differentServer.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Scanner;

public class ProducerKafkaTwo {

    public static void main(String[] args){
        Logger logger= LogManager.getLogger(ProducerKafkaTwo.class);
        KafkaProducer kafkaProducer2=null;
        kafkaProducer2= ConfigurationProducerConsumer.configProducer(kafkaProducer2, logger, "localhost:9093");
        Scanner scanner = new Scanner(System.in);
        String message;
        int count = 10;
        while(count>0){
            logger.info("Asking to type the message");
            System.out.print("Enter the message: ");
            message = scanner.nextLine();
            logger.info("Sending message to Kafka");
            ProducerMain.sendToKafka(kafkaProducer2, logger, message, "topicB");
            logger.info("Sent message to kafka");
            count--;
        }
        ProducerMain.close(kafkaProducer2, logger);
    }


}
