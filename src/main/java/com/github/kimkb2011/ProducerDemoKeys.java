package com.github.kimkb2011;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class ProducerDemoKeys  {
    public static void main( String[] args ) throws ExecutionException, InterruptedException
    {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(properties);

        // send data
        for (int i = 0; i < 10; i++) {
            // create a producer record
            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);
            // id_0 is going to partition 1
            // id_1 is going to partition 0
            // id_2 is going to 2
            // id_3 is going to 0
            // id_4 is going to 2
            // id_5 is going to 2
            // id_6 is going to 0
            // id_7 is going to 2
            producer.send(record, new Callback(){
            
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        // the record was successfully sent
                        logger.info("received new metadata:\n" + "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" + 
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get(); // block the .send() to make it synchronous - don't do this in production
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
