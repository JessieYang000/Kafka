package io.conduktor.demos.kafka;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello world!");

        //create Producer Properties
        Properties properties = new Properties();

        //connect to local host
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //connect to Conduktor playground
        properties.setProperty("bootstrap.servers", "enabling-bedbug-5399-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required");
        properties.setProperty("username", "\"ZW5hYmxpbmctYmVkYnVnLTUzOTkkIQFZV2j4jzjVMilYT-sue6b4KOx-iN69jQs\"");
        properties.setProperty("password", "\"OGFlNTFhYmMtNjM2NS00NWQ1LWFlMGUtMTYzZjI5OTBkZGFk\"");

        //set properties for producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the Producer

        //send data

        //flush and close the producer

    }
}
