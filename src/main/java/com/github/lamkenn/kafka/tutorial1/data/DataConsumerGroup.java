package com.github.lamkenn.kafka.tutorial1.data;

import com.github.lamkenn.kafka.tutorial1.BlockingQueueConsumer;
import com.github.lamkenn.kafka.tutorial1.mongo_client.MongoDBJDBC;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DataConsumerGroup implements Runnable, ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueConsumer.class);

    private final String groupId;
    private final List<String> topics;
    private final String brokers;
    private final int numberOfThreads;
    private final long startTime;
    private List<DataProcessor> consumers;
    private volatile boolean shutdown;
    private ExecutorService executor;
    private MongoDBJDBC mongoDBJDBC;
    private final KafkaConsumer<String, String> consumer;


    public DataConsumerGroup(String groupId, List<String> topics, String brokers, int numberOfThreads, long startTime) {
        this.groupId = groupId;
        this.topics = topics;
        this.brokers = brokers;
        this.shutdown = false;
        this.startTime = startTime;
        this.numberOfThreads = numberOfThreads;
        Properties prop = createConsumerConfig(brokers, groupId);
        this.consumer = new KafkaConsumer<>(prop);
        this.mongoDBJDBC = new MongoDBJDBC();
    }

    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.shutdown", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, "cgh6iop5", "RdGp960Erk2lRn6_-A1urWssEYpyTjIq");
        props.setProperty("sasl.jaas.config", jaasCfg);

        return props;
    }

    private OffsetAndTimestamp fetchOffsetByTime(TopicPartition partition , long startTime){

        Map<TopicPartition, Long> query = new HashMap<>();
        query.put(
                partition,
                startTime);

        final Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(query);
        if( offsetResult == null || offsetResult.isEmpty() ) {
            System.out.println(" No Offset to Fetch ");
            System.out.println(" Offset Size "+offsetResult.size());

            return null;
        }
        final OffsetAndTimestamp offsetTimestamp = offsetResult.get(partition);
        if(offsetTimestamp == null ){
            System.out.println("No Offset Found for partition : "+partition.partition());
        }
        return offsetTimestamp;
    }

    private void assignOffsetToConsumer( List<String> topics , long startTime){
        for (String topic : topics){
            final List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
            System.out.println("Number of Partitions : " + partitionInfoList.size());
            final List<TopicPartition> topicPartitions = new ArrayList<>();
            for (PartitionInfo pInfo : partitionInfoList) {
                TopicPartition partition = new TopicPartition(topic, pInfo.partition());
                topicPartitions.add(partition);
            }
            consumer.assign(topicPartitions);

            for(TopicPartition partition : topicPartitions ){
                OffsetAndTimestamp offSetTs = fetchOffsetByTime(partition, startTime);


                if( offSetTs == null ){
                    System.out.println("No Offset Found for partition : " + partition.partition());
                    consumer.seekToEnd(Arrays.asList(partition));
                }else {
                    System.out.println(" Offset Found for partition : " +offSetTs.offset()+" " +partition.partition());
                    System.out.println("FETCH offset success"+
                            " Offset " + offSetTs.offset() +
                            " offSetTs " + offSetTs);
                    consumer.seek(partition, offSetTs.offset());
                }
            }

        }
    }

    public void shutdown(){
        this.shutdown = true;
        this.executor.shutdown();
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        Set<TopicPartition> partitionsAssigned = consumer.assignment();
        //endOffsetsPartitionMap = consumer.endOffsets(partitionsAssigned);
    }

    @Override
    public void run() {
        executor = new ThreadPoolExecutor(this.numberOfThreads, this.numberOfThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        //consumer.subscribe(this.topics);
        assignOffsetToConsumer(topics, startTime);

        while(!shutdown){
            // get portfolio/book data from Kafka
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (final ConsumerRecord record : records) {
                logger.info("Topic: {}, Partition: {}, Offset: {}", record.topic(), record.partition(), record.offset());
                executor.submit(new DataProcessor(record, mongoDBJDBC));
            }
        }

        // shutdown service
        try {
            while(!executor.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}
