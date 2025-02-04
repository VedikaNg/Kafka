package differentServer.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class ProducerKafkaTwo {

    private KafkaProducer kafkaProducer2;

    public void start(){
        Properties propertiesProducer2 = new Properties();
        propertiesProducer2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9093");
        propertiesProducer2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        propertiesProducer2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer2 = new KafkaProducer<>(propertiesProducer2);
    }

    public void close(){
        kafkaProducer2.flush();
        kafkaProducer2.close();
    }

    public void sendToKafka(String message){
        kafkaProducer2.send(new ProducerRecord<>("topicB", message));
    }

    public static void main(String[] args){
        ProducerKafkaTwo producerKafkaTwo = new ProducerKafkaTwo();
        producerKafkaTwo.start();
        System.out.println("Producer 2");

        Scanner scanner = new Scanner(System.in);
        String message;
        int count = 10;
        while(count>0){
            System.out.print("Enter the message: ");
            message = scanner.nextLine();
            producerKafkaTwo.sendToKafka(message);
            count--;
        }


//        for(int i=0;i<15;i++){
//            String message = "Broker 2 Message "+i;
//            producerKafkaTwo.sendToKafka(i, message);
//        }

        producerKafkaTwo.close();
    }


}
