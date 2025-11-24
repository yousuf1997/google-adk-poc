package com.google.adk.configuration;

import com.google.adk.session.MongoDBSessionService;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;

@Configuration
public class MongoDBConfig {

    @Value("${database.connection.string}")
    private String connectionString;

    @Bean
    public MongoClient mongoClient() {
        return MongoClients.create(connectionString);
    }

    @Bean
    public MongoTemplate mongoTemplate(MongoClient mongoClient) {
        return new MongoTemplate(mongoClient, "google_poc_db");
    }

    /**
     * For @Transactional
     * @param dbFactory
     * @return
     */
    @Bean
    public MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Bean
    public MongoDBSessionService mongoDBSessionService(MongoTemplate mongoTemplate) {
        return new MongoDBSessionService(mongoTemplate, "adk_poc_session_and_user_state", "adk_poc_app_state");
    }

}
