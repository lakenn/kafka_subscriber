package com.github.lamkenn.kafka.tutorial1.mongo_client;

import com.google.gson.Gson;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MongoDBJDBC{

    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> collection;

    public MongoDBJDBC() {
        MongoClientURI uri = new MongoClientURI("mongodb://mantis:mantis-on-docker@10.0.1.152/mantis");
        this.mongoClient = new MongoClient(uri);
        //this.mongoClient = new MongoClient( "10.0.1.152" , 27017 );
        mongoDatabase = mongoClient.getDatabase("mantis");
    }

    public UpdateResult overlay(String sessionId, String collectionId, Integer key, Document jsonDoc){
        Document query = new Document();
        query.put("_id", key);

        jsonDoc.put("session_id", sessionId);
        Document updateObject = new Document();
        updateObject.put("$set", jsonDoc);
        UpdateOptions options = new UpdateOptions().upsert(true);
        collection = mongoDatabase.getCollection(collectionId);
        UpdateResult result =  collection.updateOne(query, updateObject, options);
        System.out.println(result);
        return result;

    }

    public UpdateResult overlay(String collectionId, String key, Document content){
        Document query = new Document();
        query.put("_id", key);

        Document updateObject = new Document();
        updateObject.put("$set", content);
        UpdateOptions options = new UpdateOptions().upsert(true);
        collection = mongoDatabase.getCollection(collectionId);
        return collection.updateOne(query, updateObject, options);

    }

    public static void main(String args[] ){
        try{
            // 连接到 mongodb 服务
            //MongoClient mongoClient = new MongoClient( "10.0.1.152" , 27017 );
            MongoClientURI uri = new MongoClientURI("mongodb://mantis:mantis-on-docker@10.0.1.152/mantis");
            MongoClient mongoClient = new MongoClient(uri);

            // 连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("mantis");
            System.out.println("Connect to database successfully");

            MongoCollection<Document> collection = mongoDatabase.getCollection("inventory2");
            System.out.println("集合 inventory 选择成功");

//            List<Integer> books = Arrays.asList(27464, 747854);
//            Document person = new Document("_id", "jo")
//                    .append("name", "Jo Bloggs")
//                    .append("address", new BasicDBObject("street", "123 Fake St")
//                            .append("city", "Faketon")
//                            .append("state", "MA")
//                            .append("zip", 12345))
//                    .append("books", books);
//
//            collection.insertOne(person);

            Document query = new Document();
            query.put("_id", "kit");

            Document update = new Document();
            update.put("name", "John");
            update.put("age", 10);
            update.append("address", new BasicDBObject("city", "HKG"));

            Document updateObject = new Document();
            updateObject.put("$set", update);

            collection.updateOne(query, updateObject);

            Document document = new Document("title", "MongoDB").
                    append("description", "database").
                    append("likes", 100.4).
                    append("by", "Fly").
                    append("timestamp", Date.from(ZonedDateTime.parse("2019-08-11T22:05:38.956Z").toInstant()));
            List<Document> documents = new ArrayList<>();
            documents.add(document);
            collection.insertMany(documents);
            System.out.println("文档插入成功");

            FindIterable<Document> findIterable = collection.find();
            MongoCursor<Document> mongoCursor = findIterable.iterator();
            while(mongoCursor.hasNext()){
                Document doc = mongoCursor.next();
                System.out.println(doc);
            }


        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }
}