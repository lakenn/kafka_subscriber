package com.github.lamkenn.kafka.tutorial1.session;

import com.github.lamkenn.kafka.tutorial1.BlockingQueueConsumer;
import com.github.lamkenn.kafka.tutorial1.KafkaConfigValidator;
import com.github.lamkenn.kafka.tutorial1.data.DataConsumerGroup;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionTopicConsumer implements Runnable, ConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueConsumer.class);

    private final String topic;
    private String latestSessionId;
    private String timestamp;
    private DataConsumerGroup dcg = null;

    private final String TEST_GROUP = "TestGroup1";
    private Consumer<String, String> consumer;
    private Map<TopicPartition, Long> endOffsetsPartitionMap;

    private Thread topicSessionThread;

    public SessionTopicConsumer(String topic) {
        this.topic = topic;
    }

    private String getClientId(){
        return "session-consumer";
    }

    @Override
    public void run() {

        /*
            To avoid setting a new group.id each time you want to read a topic from its beginning, you can disable auto commit (via enable.auto.commit = false)
            before starting the consumer for the very first time (using an unused group.id and setting auto.offset.reset = earliest).
            Additionally, you should not commit any offsets manually. Because offsets are never committed using this strategy, on restart,
            the consumer will read the topic from its beginning again.
         */

        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(GROUP_ID_CONFIG, TEST_GROUP);
        kafkaConfig.put(CLIENT_ID_CONFIG, getClientId());
        kafkaConfig.setProperty(BOOTSTRAP_SERVERS_CONFIG, "beige-gurney.srvs.cloudkafka.com:9094");
        kafkaConfig.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConfig.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaConfig.setProperty(MAX_POLL_RECORDS_CONFIG, "5000");
        kafkaConfig.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        kafkaConfig.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConfig.setProperty("security.protocol", "SASL_SSL");
        kafkaConfig.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, "cgh6iop5", "RdGp960Erk2lRn6_-A1urWssEYpyTjIq");

        //props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cgh6iop5\" password=\"9f9d2fd30d594880be998dfd27525678\";");
        kafkaConfig.setProperty("sasl.jaas.config", jaasCfg);

        KafkaConfigValidator.validate(kafkaConfig);

        consumer = new KafkaConsumer<>(kafkaConfig);
        consumer.subscribe(Arrays.asList(topic), this);

        String json2 = "{ \"name\": \"Baeldung\", \"java\": 12346 }";
        JsonObject jsonObject = new JsonParser().parse(json2).getAsJsonObject();

        // poll for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

            if (records.count() > 0 ) {
                logger.info("# of msg received: " + records.count());

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                    if (record.key().equals("session")) {
                        JSONParser parser = new JSONParser();
                        JSONObject json = null;
                        try {
                            json = (JSONObject) parser.parse((String) record.value());
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        latestSessionId = (String) json.get("Id");
                        timestamp = (String) json.get("Timestamp");

                        // if lastSessionId != storedSessionId
                        // restarted  = True

                    }
                }

                if (dcg != null){

                    dcg.shutdown();
                    try {
                        topicSessionThread.join();
                    } catch (InterruptedException e) {
                        logger.info(String.valueOf(e));
                    }

                    // clean up work
                    // flush mongo

                }

                ZonedDateTime lastSessionTimestamp = ZonedDateTime.parse(timestamp);
                long startTime = lastSessionTimestamp.toInstant().toEpochMilli();

                // processed all the "session" msg in the previous batch
                List<String> topics = Arrays.asList("cable-Holdings_Combined_SEGA");
                String groupId = "Ken-consumer-group2";
                String brokers = "beige-gurney.srvs.cloudkafka.com:9094";
                dcg = new DataConsumerGroup(groupId, topics, brokers, 8, startTime);
                topicSessionThread = new Thread(dcg);
                topicSessionThread.start();
            }


        }
    };

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        Set<TopicPartition> partitionsAssigned = consumer.assignment();
        endOffsetsPartitionMap = consumer.endOffsets(partitionsAssigned);
    }

}
