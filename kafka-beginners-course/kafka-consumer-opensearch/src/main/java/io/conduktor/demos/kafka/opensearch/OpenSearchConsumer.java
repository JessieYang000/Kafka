package io.conduktor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static RestHighLevelClient createOpenSearchClient() {
//        String connString = "http://localhost:9200";
        String connString = "https://6ghx8guknp:jkwpger0ob@kafka-course-7426833330.us-east-1.bonsaisearch.net:443";
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        //create Producer Properties
        Properties properties = new Properties();

        //connect to Conduktor playground
        String path = "config.properties";// path to config.properties from the current working directory
        try (InputStream input = new FileInputStream(path)) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        String groupId = "consumer-opensearch-demo";
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "latest"); //there are another two options: none and latest

        //create a consumer
        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        //create the Open Search client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create the Kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //main code logic
        //we need to create the index on OpenSearch if it doesn't exist
        try(openSearchClient; consumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia Index has been created.");
            } else {
                log.info("The wikimedia Index has already exist.");
            }

            //set the consumer to subscribe a topic
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s).");

                //send the record to the OpenSearch one by one
                for(ConsumerRecord<String, String> record : records) {
                    //strategy 1: define an ID using the Kafka record coordinates
//                        String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try {
                         //strategy 2: define an ID by extracting from the record value
                        String id = extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info(indexResponse.getId());
                    } catch (Exception e) {

                    }
                }
            }
        }

        //close things
    }
}
