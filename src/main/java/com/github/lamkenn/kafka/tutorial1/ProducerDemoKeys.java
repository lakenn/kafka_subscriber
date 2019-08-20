package com.github.lamkenn.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "10.0.1.152:9092";

//        // create Producer properties
//        Properties properties = new Properties();
//
//        properties.setProperty("bootstrap.servers", bootstrapServers);
//
//        // what type of value you are sending to kafka
//        // how shold it be serialized to byte
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());
//

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // message goes to  partition (round robin) because we didn't use any key
        for (int i = 0; i < 10; i++)
        {
            String topic = "first_topic";
            String value = "hello word " + i;
            String key = "id_" + i;

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            logger.info("Key: " + key);

            // send data -- asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute every time a record is successfully sent or an exception is thrown

                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" + "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);

                    }

                }
            }).get();   //block the .send() to make it synchronous -- don't do this in production //.get force it to be synchronuous => will have
        }
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
