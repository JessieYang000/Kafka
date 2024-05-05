package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("I am a kafka Producer！");

        //create Producer Properties
        Properties properties = new Properties();

        //connect to local host
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        String path = "config.properties";// path to config.properties from the current working directory
        try (InputStream input = new FileInputStream(path)) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

//        properties.setProperty("username", "\"ZW5hYmxpbmctYmVkYnVnLTUzOTkkIQFZV2j4jzjVMilYT-sue6b4KOx-iN69jQs\"");
//        properties.setProperty("password", "\"OGFlNTFhYmMtNjM2NS00NWQ1LWFlMGUtMTYzZjI5OTBkZGFk\"");

        //set properties for producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create kafka producer and producer record
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for(int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                //send data --asynchronous
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello World with Keys" + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                kafkaProducer.send(producerRecord, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //the record was successfully sent
                            log.info(
                                    "Key: " + key +
                                    "| Partition: " + metadata.partition()
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
