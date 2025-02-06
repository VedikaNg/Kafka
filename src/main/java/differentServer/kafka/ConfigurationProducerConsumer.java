package differentServer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class ConfigurationProducerConsumer {

    public static KafkaProducer configProducer(KafkaProducer kafkaProducer, Logger logger, String serverConfig){
        logger.info("Setting the properties in "+ kafkaProducer);
        Properties propertiesProducer2 = new Properties();
        propertiesProducer2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,serverConfig);
        propertiesProducer2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        propertiesProducer2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<>(propertiesProducer2);
        logger.info("Properties set in "+ kafkaProducer);
        return kafkaProducer;
    }

    public static KafkaConsumer configConsumer(KafkaConsumer kafkaConsumer, String group, String serverConfig, Logger logger){
        Properties propertiesConsumer = new Properties();
        logger.info("Loading the configuration in "+ kafkaConsumer);
        propertiesConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,serverConfig);
        propertiesConsumer.put(ConsumerConfig.GROUP_ID_CONFIG,group);
        propertiesConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        propertiesConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumer = new KafkaConsumer<>(propertiesConsumer);
        return kafkaConsumer;
    }
}
