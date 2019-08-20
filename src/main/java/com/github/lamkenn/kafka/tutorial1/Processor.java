package com.github.lamkenn.kafka.tutorial1;

import com.github.lamkenn.kafka.tutorial1.mongo_client.MongoDBJDBC;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

class Processor<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class);

    private volatile boolean stopped = false;

    private long endOffset;
    private final BlockingQueue<ConsumerRecord<K, V>> queue;
    private final ConsumerRecordRelay<K, V> relay;
    private final java.util.function.Consumer<V> action;
    private final TopicPartition topicPartition;

    private String sessionId;
    private String timestamp;

    private final MongoDBJDBC mongoDBJDBC;
    
    Processor(TopicPartition topicPartition, ConsumerRecordRelay<K, V> relay, java.util.function.Consumer<V> action,
              int queueSize, long endOffset) {
        this.endOffset = endOffset;
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.relay = relay;
        this.action = action;
        this.topicPartition = topicPartition;
        mongoDBJDBC = new MongoDBJDBC();
    }

    @Override
    public void run() {
        Thread.currentThread().setName(topicPartition.toString());
        logger.info("Processor for {} started", topicPartition);
        JSONParser parser = new JSONParser();

        try {
            while (!stopped) {
                ConsumerRecord<K, V> record = queue.take();
                action.accept(record.value());
                String key = (String)record.key();
                String value = (String)record.value();

                if ("session".equals(key)){
                    JSONObject jsonObject = (JSONObject) parser.parse(value);

                    sessionId = (String)jsonObject.get("Id");
                    timestamp = (String)jsonObject.get("Timestamp");


                }
                //String[] bits = key.split("-");
                //String holdingId = bits[bits.length-1];
                //Document content = Document.parse(value);
                //mongoDBJDBC.overlay(holdingId, content);
                relay.setOffset(record);

                // reach the end of the msg queue
                if (record.offset() == endOffset-1){
                    //
                    System.out.println(sessionId);
                    System.out.println(timestamp);
                }
            }
        } catch (InterruptedException ignored) {
            logger.debug("Processor for {} interrupted while waiting for messages", topicPartition);
        } catch (Exception ex) {
            logger.error("Exception during processing {}. Stopping!", topicPartition, ex);
        }
        stop();
        queue.clear();
        logger.info("Processor for {} stopped", topicPartition);
    }

    public void stop() {
        stopped = true;
    }

    public void queue(ConsumerRecord<K, V> record) throws InterruptedException {
        queue.put(record);
    }

    public boolean isStopped() {
        return stopped;
    }
}