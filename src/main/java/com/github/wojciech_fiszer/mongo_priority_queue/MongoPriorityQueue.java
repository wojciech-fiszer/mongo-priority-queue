package com.github.wojciech_fiszer.mongo_priority_queue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.client.model.Sorts.ascending;

public class MongoPriorityQueue {

    private final MongoClient mongoClient;
    private final String databaseName;
    private final String collectionName;
    private final long expireFinishedMessagesAfterSeconds;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final CountDownLatch initializationCountDownLatch = new CountDownLatch(1);

    public MongoPriorityQueue(MongoClient mongoClient, String databaseName, String collectionName) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        expireFinishedMessagesAfterSeconds = TimeUnit.DAYS.toSeconds(1);
    }

    public MongoPriorityQueue(MongoClient mongoClient, String databaseName, String collectionName, int expireFinishedMessagesAfterSeconds) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.expireFinishedMessagesAfterSeconds = expireFinishedMessagesAfterSeconds;
    }

    public void initialize() throws InterruptedException {
        if (!initialized.getAndSet(true)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            database.createCollection(collectionName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            // index for querying for next polled item(s)
            collection.createIndex(Indexes.ascending("started_at"));
            // index for sorting when querying for next polled item(s)
            collection.createIndex(Indexes.ascending("priority", "queued_at"));
            collection.createIndex(
                    Indexes.ascending("finished_at"),
                    new IndexOptions().expireAfter(expireFinishedMessagesAfterSeconds, TimeUnit.SECONDS)
            );
            initializationCountDownLatch.countDown();
        } else {
            initializationCountDownLatch.await();
        }
    }

    public void push(String payload, int priority) {
        Document document = new Document()
                .append("priority", priority)
                .append("queued_at", new Date())
                .append("started_at", null)
                .append("finished_at", null)
                .append("payload", payload);
        mongoClient.getDatabase(databaseName).getCollection(collectionName).insertOne(document);
    }

    public Optional<Message> poll() {
        Set<String> excludedGroups = Set.of(); // TODO
        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        Document document = collection.findOneAndUpdate(
                new Document("started_at", null).append("group", new Document("$nin", excludedGroups)),
                new Document("$set", new Document("started_at", new Date())),
                new FindOneAndUpdateOptions().sort(ascending("priority", "queued_at"))
        );
        if (document == null) return Optional.empty();
        System.out.println(document);
        return Optional.of(new Message(document.getString("payload")) {
            @Override
            public void acknowledge() {
                collection.findOneAndUpdate(
                        new Document("_id", document.getObjectId("_id")),
                        new Document("$set", new Document("finished_at", new Date()))
                );
            }

            @Override
            public void retry() {
                collection.findOneAndUpdate(
                        new Document("_id", document.getObjectId("_id")),
                        new Document("$set", new Document("started_at", null))
                );
            }
        });
    }

    static abstract class Message {

        private final String payload;

        public Message(String payload) {
            this.payload = payload;
        }

        public String getPayload() {
            return payload;
        }

        public abstract void acknowledge();

        public abstract void retry();
    }
}
