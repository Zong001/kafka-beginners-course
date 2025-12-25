package io.conductor.demo.kafka;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("H W");

        //create Prod properties

        Properties properties = new Properties();

        //local host
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //playground
//        properties.setProperty("security.protocal","SASL_SSL");
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //set prod prop
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());



        //create Prod

        //send data

        //flush and close Prod
    }
}
