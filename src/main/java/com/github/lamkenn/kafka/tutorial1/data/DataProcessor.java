package com.github.lamkenn.kafka.tutorial1.data;

import com.github.lamkenn.kafka.tutorial1.BlockingQueueConsumer;
import com.github.lamkenn.kafka.tutorial1.mongo_client.MongoDBJDBC;
import com.google.gson.Gson;
import com.mongodb.client.result.UpdateResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.Map;

public class DataProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueConsumer.class);
    private ConsumerRecord record;
    private MongoDBJDBC mongoJDBC;

    public DataProcessor(ConsumerRecord record, MongoDBJDBC mongoJDBC) {
        this.record = record;
        this.mongoJDBC = mongoJDBC;
    }

    private Document convertJsonToDocument(String jsonData){
        Gson gson = new Gson();
        Map<String, String> contentMap = gson.fromJson(jsonData, Map.class);

        Document jsonDoc = new Document();

        Pattern numericPattern = Pattern.compile(" *[+\\-]?(?:0|[1-9]\\d*)(?:\\.\\d*)?(?:[eE][+\\-]?\\d+)?");
        Pattern dateTimePattern = Pattern.compile("\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.?\\d{0,3}?Z");

        Matcher matcher;

        for(Map.Entry<String, String> entry : contentMap.entrySet()){


            try {
                if (numericPattern.matcher(entry.getValue()).matches()){
                    jsonDoc.put(entry.getKey(), new BigDecimal(entry.getValue().replace(",", "")));
                }
                else if (dateTimePattern.matcher(entry.getValue()).matches()){
                    jsonDoc.put(entry.getKey(), Date.from(ZonedDateTime.parse(entry.getValue()).toInstant()));
                }
                else{
                    jsonDoc.put(entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return jsonDoc;
    }

    @Override
    public void run() {
        String key = ((String)record.key());

        String sessionId = key.substring(0, 36);
        Integer holdingId = Integer.parseInt(key.substring(37));

        String topic = (String)record.topic();
        String jsonData = (String)record.value();

        String collectionId = topic.split("-")[1];

        Document jsonDoc = convertJsonToDocument(jsonData);
        UpdateResult result = mongoJDBC.overlay(sessionId, collectionId, holdingId, jsonDoc);
        logger.info("Updated: " + result.getModifiedCount());
    }
}
