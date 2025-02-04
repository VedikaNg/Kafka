package differentServer.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerKafkaOne {
    private KafkaProducer kafkaProducer1;

    public void start(){
        Properties propertiesProducer1 = new Properties();
        propertiesProducer1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        propertiesProducer1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        propertiesProducer1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
//        propertiesProducer1.put(ProducerConfig.ACKS_CONFIG,"");
        kafkaProducer1 = new KafkaProducer<>(propertiesProducer1);
    }

    public void close(){
        kafkaProducer1.flush();
        kafkaProducer1.close();
    }

    public void sendToKafka(int i, String message){
        kafkaProducer1.send(new ProducerRecord<>("topicA",Integer.toString(i),message));
    }

    public static void main(String[] args){
        ProducerKafkaOne producerKafkaOne = new ProducerKafkaOne();
        producerKafkaOne.start();

        for(int i=0;i<10;i++){
            String message = "Broker 1 Message "+i;
            producerKafkaOne.sendToKafka(i, message);
        }

        producerKafkaOne.close();
    }
}
