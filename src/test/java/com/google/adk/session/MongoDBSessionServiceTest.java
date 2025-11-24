package com.google.adk.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.JsonBaseModel;
import com.google.adk.sessions.*;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class MongoDBSessionServiceTest {

    private MongoTemplate mongoTemplate;
    private MongoDBSessionService service;
    private ObjectMapper mapper;

    private static final String COLLECTION = "session_user_state";
    private static final String APP_COLLECTION = "app_state";

    @BeforeEach
    void setup() {
        mongoTemplate = mock(MongoTemplate.class);
        mapper = JsonBaseModel.getMapper();
        service = new MongoDBSessionService(mongoTemplate, COLLECTION, APP_COLLECTION);
    }

    // -------------------------------------------------------------------------
    // FIXTURE — Sample MongoDB document
    // -------------------------------------------------------------------------

    private Document sampleSessionDoc() {
        return Document.parse("""
        {
          "appName": "adk-poc",
          "userId": "user1234",
          "userStates": [],
          "sessions": [
            {
              "sessionId": "bfe69006-eb65-4015-a754-4887d9019fac",
              "session": {
                "id": "bfe69006-eb65-4015-a754-4887d9019fac",
                "appName": "adk-poc",
                "userId": "user1234",
                "state": {},
                "events": [
                  {
                    "id": "10c37df3-2686-44d4-beed-ab5384ec13a7",
                    "invocationId": "e-54ca7a57-69b6-4642-a761-5d9df8dfa4f1",
                    "author": "user",
                    "content": { "role": "user", "parts": [{ "text": "When did India got independence?" }] },
                    "actions": { "stateDelta": {}, "artifactDelta": {}, "requestedAuthConfigs": {} },
                    "timestamp": 1763996463557
                  },
                  {
                    "id": "90115dad-76a1-4957-ad37-5900a9d9abbd",
                    "invocationId": "e-bd837b72-f0f7-4598-934d-65a0ac48ab28",
                    "author": "History Expert Agent",
                    "content": { "role": "model", "parts": [{ "text": "India achieved independence..." }] },
                    "actions": { "stateDelta": {}, "artifactDelta": {}, "requestedAuthConfigs": {} },
                    "timestamp": 1763996472111
                  }
                ],
                "lastUpdateTime": 1763996506.0
              }
            }
          ]
        }
        """);
    }

    // -------------------------------------------------------------------------
    // createSession
    // -------------------------------------------------------------------------

    @Test
    void testCreateSession_createsNewSessionAndWritesDocument() {

        Document savedDoc = new Document();
        when(mongoTemplate.findAndModify(any(), any(), any(), eq(Document.class), eq(COLLECTION)))
                .thenReturn(savedDoc);

        var session = service
                .createSession("adk-poc", "user1234", null, null)
                .blockingGet();

        assertThat(session).isNotNull();
        assertThat(session.appName()).isEqualTo("adk-poc");
        assertThat(session.userId()).isEqualTo("user1234");
        assertThat(session.events()).isEmpty();

        verify(mongoTemplate).findAndModify(any(), any(), any(), eq(Document.class), eq(COLLECTION));
    }

    // -------------------------------------------------------------------------
    // getSession
    // -------------------------------------------------------------------------

    @Test
    void testGetSession_returnsSessionWithEvents() {
        when(mongoTemplate.find(any(), eq(Document.class), eq(COLLECTION)))
                .thenReturn(List.of(sampleSessionDoc()));

        var sessionOpt = service
                .getSession("adk-poc", "user1234", "bfe69006-eb65-4015-a754-4887d9019fac", Optional.empty())
                .blockingGet();

        assertThat(sessionOpt).isNotNull();
        assertThat(sessionOpt.events()).hasSize(2);
    }

    @Test
    void testGetSession_numRecentEventsFiltersCorrectly() {
        when(mongoTemplate.find(any(), eq(Document.class), eq(COLLECTION)))
                .thenReturn(List.of(sampleSessionDoc()));

        var config = GetSessionConfig.builder().numRecentEvents(1).build();

        var session = service
                .getSession("adk-poc", "user1234", "bfe69006-eb65-4015-a754-4887d9019fac", Optional.of(config))
                .blockingGet();

        assertThat(session.events()).hasSize(1);
        assertThat(session.events().getFirst().id()).isEqualTo("90115dad-76a1-4957-ad37-5900a9d9abbd");
    }

    @Test
    void testListEvents_returnsAllEvents() {
        when(mongoTemplate.find(any(), eq(Document.class), eq(COLLECTION)))
                .thenReturn(List.of(sampleSessionDoc()));

        var eventsResp = service
                .listEvents("adk-poc", "user1234", "bfe69006-eb65-4015-a754-4887d9019fac")
                .blockingGet();

        assertThat(eventsResp.events()).hasSize(2);
    }

    // -------------------------------------------------------------------------
    // deleteSession
    // -------------------------------------------------------------------------

    @Test
    void testDeleteSession_removesSession() {
        when(mongoTemplate.updateFirst(any(), any(), eq(COLLECTION)))
                .thenReturn(null);

        service.deleteSession("adk-poc", "user1234", "xyz").blockingAwait();

        verify(mongoTemplate).updateFirst(any(), any(), eq(COLLECTION));
    }

}
