package com.github.wojciech_fiszer.mongo_priority_queue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Sorts.ascending;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MongoPriorityQueueTest {

    @Container
    public MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2");

    @Test
    void shouldPushAndPollMessage() throws InterruptedException {
        try (MongoClient mongoClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            // given
            String databaseName = uuid();
            String collectionName = uuid();
            MongoPriorityQueue queue = new MongoPriorityQueue(mongoClient, databaseName, collectionName);
            queue.initialize();
            String payload = uuid();

            // when
            queue.push(payload, 1);

            // then
            Optional<MongoPriorityQueue.Message> message = queue.poll();
            assertTrue(message.isPresent());
            assertEquals(payload, message.get().getPayload());
            assertTrue(queue.poll().isEmpty());
        }
    }

    @Test
    void shouldPushAndPollMessageWithProperPriorityAndQueueingOrder() throws InterruptedException {
        try (MongoClient mongoClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            // given
            String databaseName = uuid();
            String collectionName = uuid();
            MongoPriorityQueue queue = new MongoPriorityQueue(mongoClient, databaseName, collectionName);
            queue.initialize();

            // when
            queue.push("old-one", 1);
            queue.push("old-two", 2);
            queue.push("new-two", 2);
            queue.push("new-one", 1);

            // then
            Optional<MongoPriorityQueue.Message> message = queue.poll();
            assertTrue(message.isPresent());
            assertEquals("old-one", message.get().getPayload());
            message = queue.poll();
            assertTrue(message.isPresent());
            assertEquals("new-one", message.get().getPayload());
            message = queue.poll();
            assertTrue(message.isPresent());
            assertEquals("old-two", message.get().getPayload());
            message = queue.poll();
            assertTrue(message.isPresent());
            assertEquals("new-two", message.get().getPayload());
            assertTrue(queue.poll().isEmpty());
        }
    }

    @Test
    void shouldAcknowledgeAfterInvokingAcknowledgeOnMessage() throws InterruptedException {
        try (MongoClient mongoClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            // given
            String databaseName = uuid();
            String collectionName = uuid();
            MongoPriorityQueue queue = new MongoPriorityQueue(mongoClient, databaseName, collectionName, 1);
            queue.initialize();
            String payload = uuid();
            queue.push(payload, 1);
            Optional<MongoPriorityQueue.Message> message = queue.poll();
            assertTrue(message.isPresent());
            assertEquals(payload, message.get().getPayload());

            // when
            message.get().acknowledge();

            // then
            assertTrue(queue.poll().isEmpty());
            // the TTL background thread runs every 60 seconds
            await().atMost(60, TimeUnit.SECONDS).until(() -> mongoClient.getDatabase(databaseName).getCollection(collectionName).countDocuments() == 0);
        }
    }

    @Test
    void shouldRetryAfterInvokingRetryOnMessage() throws InterruptedException {
        try (MongoClient mongoClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            // given
            String databaseName = uuid();
            String collectionName = uuid();
            MongoPriorityQueue queue = new MongoPriorityQueue(mongoClient, databaseName, collectionName);
            queue.initialize();
            String payload = uuid();
            queue.push(payload, 1);
            Optional<MongoPriorityQueue.Message> message = queue.poll();
            assertTrue(message.isPresent());
            assertEquals(payload, message.get().getPayload());
            assertTrue(queue.poll().isEmpty());

            // when
            message.get().retry();

            // then
            message = queue.poll();
            assertTrue(message.isPresent());
            assertEquals(payload, message.get().getPayload());
            assertTrue(queue.poll().isEmpty());
        }
    }

    @Test
    void verifyQuery() throws InterruptedException {
        try (MongoClient mongoClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            // given
            String databaseName = uuid();
            String collectionName = uuid();
            MongoPriorityQueue queue = new MongoPriorityQueue(mongoClient, databaseName, collectionName);
            queue.initialize();
            Document explain = mongoClient.getDatabase(databaseName).getCollection(collectionName)
                    .find(new Document("started_at", null))
                    .sort(ascending("priority", "queued_at"))
                    .limit(1)
                    .explain();
            System.out.println(explain.toJson());
        }
    }

    private static String uuid() {
        return UUID.randomUUID().toString();
    }
}