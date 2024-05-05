package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello world!");
//        System.out.println("Current working directory: " + System.getProperty("user.dir"));

        //create Producer Properties
        Properties properties = new Properties();
        String path = "config.properties";// Relative path to config.properties from the current Java file
        try (InputStream input = new FileInputStream(path)) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        //connect to local host
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set properties for producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create kafka producer and producer record
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        //send data --asynchronous
        kafkaProducer.send(producerRecord);

        //tell the producer to send all data and block until done -- synchronous
        kafkaProducer.flush();

        //flush and close the producer
        kafkaProducer.close(); //when call close(), it will also call flush()

    }
}
