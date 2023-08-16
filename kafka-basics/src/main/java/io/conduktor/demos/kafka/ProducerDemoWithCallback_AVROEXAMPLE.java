package io.conduktor.demos.kafka;

import io.conduktor.demos.kafka.avro.User;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
public class ProducerDemoWithCallback_AVROEXAMPLE {

    private static final Logger log= LoggerFactory.getLogger(ProducerDemoWithCallback_AVROEXAMPLE.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a kafka producer!");

        Properties properties=new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //properties.setProperty("security.protocol", "SASL_SSL");
        //properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"GyFxCmAp7umOiz0DT58nx\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJHeUZ4Q21BcDd1bU9pejBEVDU4bngiLCJvcmdhbml6YXRpb25JZCI6NzQ2OTIsInVzZXJJZCI6ODY5MDgsImZvckV4cGlyYXRpb25DaGVjayI6ImZhOGM3MDgzLTZhYmMtNDQ4Zi04Nzg2LWE0NGQ4ZjM5ZDdhYiJ9fQ.aFxmMVRfl9mu1AbuI6ZAD6OZMYIodhR9x8APQBBFv5s\";");
        //properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        //properties.setProperty("schema.registry.url", "http://kafka-schema-registry:8081");


        //properties.setProperty("batch.size", "400");
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        KafkaProducer<String, User> producer=new KafkaProducer<>(properties);


        User user = new User();
        user.setName("Abhi");
        user.setAge(30);


        for(int j=0;j<10;j++){

            for(int i=0;i<30;i++){

                ProducerRecord<String, User> producerRecord= new ProducerRecord<>("demo_java", user);

                producer.send(producerRecord, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null){
                            log.info("Received new metadata \n" +
                                     "Topic: " + recordMetadata.topic() + "\n" +
                                     "Partition: " + recordMetadata.partition() + "\n" +
                                     "Offset: " + recordMetadata.offset() + "\n" +
                                     "Timestamp: " + recordMetadata.timestamp() + "\n"
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

        producer.flush();
        producer.close();

    }
}
