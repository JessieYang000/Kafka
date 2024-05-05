package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("I am a kafka Producer！");

        //connect to local host
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //create Producer Properties
        Properties properties = new Properties();

        String path = "config.properties";// path to config.properties from the current working directory
        try (InputStream input = new FileInputStream(path)) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        //set properties for producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "400");
        //set the partitioner to round robin, but not recommended
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create kafka producer and producer record
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for(int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world with Callback" + i);

                //send data --asynchronous
                kafkaProducer.send(producerRecord, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //the record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partitions: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + ";"
                            );
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



        //tell the producer to send all data and block until done -- synchronous
        kafkaProducer.flush();

        //flush and close the producer
        kafkaProducer.close(); //when call close(), it will also call flush()

    }
}
