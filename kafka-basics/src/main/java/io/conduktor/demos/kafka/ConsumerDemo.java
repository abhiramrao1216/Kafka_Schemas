package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemo {

    private static final Logger log= LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {

        log.info("I am a kafka consumer!");

        String groupId= "my-java-application";
        String topic="demo_java";

        Properties properties=new Properties();

        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"GyFxCmAp7umOiz0DT58nx\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJHeUZ4Q21BcDd1bU9pejBEVDU4bngiLCJvcmdhbml6YXRpb25JZCI6NzQ2OTIsInVzZXJJZCI6ODY5MDgsImZvckV4cGlyYXRpb25DaGVjayI6ImZhOGM3MDgzLTZhYmMtNDQ4Zi04Nzg2LWE0NGQ4ZjM5ZDdhYiJ9fQ.aFxmMVRfl9mu1AbuI6ZAD6OZMYIodhR9x8APQBBFv5s\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer=new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while (true) {

           log.info("Polling");

           ConsumerRecords<String, String> records=
                   consumer.poll(Duration.ofMillis(1000));

           for(ConsumerRecord<String,String> record: records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
           }
        }

    }
}
