package com.debashis.twitter.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaTwitterMessageProducer {

    public static KafkaProducer<String, String> getProducer() {
        if(producer==null){
            initialize();
        }
        return producer;
    }

    private static KafkaProducer<String, String> producer;
    private static void initialize() {

        String bootstrap = " ec2-65-0-45-93.ap-south-1.compute.amazonaws.com:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        //properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100000");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        System.out.println("Producer config created");
        producer = new KafkaProducer<String, String>(properties);

    }
    static {
        initialize();
    }

}
