package com.debashis.apache.kafka.simple.clint;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class KafkaMessageReceiver {
    public static void main(String[] args) {
        String bootstrap = " ec2-65-0-45-93.ap-south-1.compute.amazonaws.com:9092";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        //properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //consumer.subscribe(new ArrayList<String>(){{add("first_topic");}} );
        consumer.subscribe(Collections.singleton("first_topic") );
        //
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(300));

            for (ConsumerRecord<String, String> record : records){
                System.out.println("key:"+record.key()+", value:"+record.value());
                System.out.println("partition:"+record.partition()+", offset:"+record.offset());
                System.out.println("-----end for within");
            }
            System.out.println("-----end for out");
        }
    }
}
