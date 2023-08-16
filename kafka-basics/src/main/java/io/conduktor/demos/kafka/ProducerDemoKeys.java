package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoKeys {

    private static final Logger log= LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a kafka producer!");

        Properties properties=new Properties();

        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"GyFxCmAp7umOiz0DT58nx\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJHeUZ4Q21BcDd1bU9pejBEVDU4bngiLCJvcmdhbml6YXRpb25JZCI6NzQ2OTIsInVzZXJJZCI6ODY5MDgsImZvckV4cGlyYXRpb25DaGVjayI6ImZhOGM3MDgzLTZhYmMtNDQ4Zi04Nzg2LWE0NGQ4ZjM5ZDdhYiJ9fQ.aFxmMVRfl9mu1AbuI6ZAD6OZMYIodhR9x8APQBBFv5s\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //properties.setProperty("batch.size", "400");

        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());



        KafkaProducer<String, String> producer=new KafkaProducer<>(properties);

        for(int j=0;j<2;j++) {

            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world " + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            log.info("Key: " + key + " | Partition: " + recordMetadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });

            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        producer.flush();

        producer.close();

    }
}
