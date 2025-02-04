package differentServer.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class ProducerKafkaOne {
    private KafkaProducer kafkaProducer1;

    public void start(){
        Properties propertiesProducer1 = new Properties();
        propertiesProducer1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        propertiesProducer1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        propertiesProducer1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer1 = new KafkaProducer<>(propertiesProducer1);
    }

    public void close(){
        kafkaProducer1.flush();
        kafkaProducer1.close();
    }

    public void sendToKafka(String message){
        kafkaProducer1.send(new ProducerRecord<>("topicA",message));
    }

    public static void main(String[] args){
        ProducerKafkaOne producerKafkaOne = new ProducerKafkaOne();
        producerKafkaOne.start();
        System.out.println("Producer 1");
        Scanner scanner = new Scanner(System.in);
        String message;
        int count =10;
        while(count>0){
            System.out.print("Enter the message: ");
            message = scanner.nextLine();
            producerKafkaOne.sendToKafka(message);
            count--;
        }
//        for(int i=0;i<10;i++){
//            String message = "Broker 1 Message "+i;
//            producerKafkaOne.sendToKafka(i, message);
//        }

        producerKafkaOne.close();
    }
}
