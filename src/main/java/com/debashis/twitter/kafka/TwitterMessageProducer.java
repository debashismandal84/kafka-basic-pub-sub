package com.debashis.twitter.kafka;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterMessageProducer {

    private static String API_Key = "Dhmo1pwAbMk72FpXk3NlVi9yd";
    private static String API_Secrete_Key = "7HeyLZBnGX5SL8z82PNYwkJpYOdm8ftTLOoWa58AOd4lP7wVpX";
    private static String Access_Token = "151119340-urecM8JUifc22ZrwFm0KD7tQqTSlahuzZvW2Cu9c";
    private static String Access_Token_Secrete = "oUBGmzrntVwL6a4Uv8Sj92f3UTp5crISaNX11HkWMZvMj";

    private static Hosts hosebirdHosts;
    private static Authentication hosebirdAuth;
    private static StatusesFilterEndpoint hosebirdEndpoint;
    private static BlockingQueue<String> msgQueue;

    private static KafkaProducer<String, String> kafkaProducer = KafkaTwitterMessageProducer.getProducer();
    public static void main(String[] args) {

            createTwiterCon();
            Client twiterClient = buildClient();
            twiterClient.connect();

            Runtime.getRuntime().addShutdownHook(new Thread(()-> {
                twiterClient.stop();
            kafkaProducer.flush();
            kafkaProducer.close();
            System.out.println("closed all the open connections");
        }));
        try {
            while (!twiterClient.isDone()) {
                String msg = msgQueue.poll(20, TimeUnit.SECONDS);
                if(!Strings.isNullOrEmpty(msg))
                 System.out.println("message from Twitter:"+msg);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_topic",
                        msg);
                System.out.println("Producer Record created");

                kafkaProducer.send(record);
                System.out.println("Producer message sent");

                kafkaProducer.flush();

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            twiterClient.stop();
        }

    }

    private static void createTwiterCon() {
        msgQueue = new LinkedBlockingQueue<>(
                1000);
       // BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("afganistan");
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        hosebirdAuth = new OAuth1(API_Key, API_Secrete_Key, Access_Token, Access_Token_Secrete);
    }

    public static Client buildClient(){
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)

                .processor(new StringDelimitedProcessor(msgQueue));
               // .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        return  builder.build();
// Attempts to establish a connection.
       // hosebirdClient.connect();
    }
}
