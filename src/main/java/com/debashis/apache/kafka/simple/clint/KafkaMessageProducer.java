package com.debashis.apache.kafka.simple.clint;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaMessageProducer {
    public static void kafkaProducer() {

        String bootstrap = " ec2-65-0-45-93.ap-south-1.compute.amazonaws.com:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        //properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        System.out.println("Producer config created");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i=0; i<10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Key_"+i,
                    "message from Java Producer - "+i);
            System.out.println("Producer Record created");

            producer.send(record);
            System.out.println("Producer message sent");

            producer.flush();
        }
        producer.close();
    }
}
