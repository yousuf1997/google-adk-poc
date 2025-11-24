package com.google.adk.session;

import com.google.adk.agents.LlmAgent;
import com.google.adk.runner.MongoDBSessionRunner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.mongodb.test.autoconfigure.DataMongoTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import java.util.Optional;

@DataMongoTest
@Import(MongoDBSessionService.class)
public class MongoDBSessionServiceTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MongoDBSessionService mongoDBSessionService;

    @MockitoBean
    private MongoDBSessionRunner runner;

    private final String sessionAndUserCollectionName = "session_and_user_state";
    private final String appStateCollectionName = "app_state";

    @BeforeEach
    void setUp() {
        if (!mongoTemplate.collectionExists(sessionAndUserCollectionName)) {
            mongoTemplate.createCollection(sessionAndUserCollectionName);
        }
        if (!mongoTemplate.collectionExists(appStateCollectionName)) {
            mongoTemplate.createCollection(appStateCollectionName);
        }
    }

    @Test
    public void lifecycle_noSession() {

        // Assertions
        Assertions.assertNull(mongoDBSessionService.getSession("app-name", "user-id", "session-id", Optional.empty()).blockingGet());
        Assertions.assertTrue(mongoDBSessionService.listSessions("app-name", "user-id").blockingGet().sessions().isEmpty());
        Assertions.assertNull(mongoDBSessionService.listEvents("app-name", "user-id", "session-id").blockingGet());
    }
}