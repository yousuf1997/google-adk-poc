package com.mohamed.google_adk_poc.session;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.JsonBaseModel;
import com.google.adk.events.Event;
import com.google.adk.sessions.*;
import com.google.adk.utils.CollectionUtils;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import org.bson.Document;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final public class MongoDBSessionService  implements BaseSessionService {

    /***
     * Document structure
     *  - SESSION_USER_STATE -
     * {
     *     "appName" : "",
     *     "userId" : "",
     *     "stateKey" : "",
     *     "sessions" : [
     *       {
     *           "sessionId" : "",
     *           "session" : {
     *
     *           }
     *       }
     *     ],
     *     "states" : [
     *        {
     *            "stateKey": "",
     *           "stateValue" : {}
     *        }
     *     ]
     * }
     * -- APP STATE
     * {
     *     "appName" : "",
     *     "states" : [
     *       {
     *           "stateKey" : "",
     *           "stateValue" : {}
     *       }
     *     ]
     * }
     *
     */

    private final MongoTemplate mongoTemplate;
    private final String sessionUserStateCollection;
    private final String appStateCollection;
    private final ObjectMapper objectMapper;
    private final InMemorySessionService inMemorySessionService;

    public MongoDBSessionService(MongoTemplate mongoTemplate, String sessionUserStateCollection, String appStateCollection) {
        this.mongoTemplate = mongoTemplate;
        this.sessionUserStateCollection = sessionUserStateCollection;
        this.appStateCollection = appStateCollection;
        this.objectMapper = JsonBaseModel.getMapper();

        this.inMemorySessionService = new InMemorySessionService();
    }


    @Override
    public Single<Session> createSession(String appName, String userId, @Nullable ConcurrentMap<String, Object> state, @Nullable String sessionId) {

        Objects.requireNonNull(appName, "appName cannot be null");
        Objects.requireNonNull(userId, "userId cannot be null");

        String resolvedSessionId =  Optional.ofNullable(sessionId)
                                    .map(String::trim)
                                    .filter(s -> !s.isEmpty())
                                    .orElseGet(() -> UUID.randomUUID().toString());

        // Ensure state map and events list are mutable for the new session
        ConcurrentMap<String, Object> initialState = (state == null) ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(state);
        List<Event> initialEvents = new ArrayList<>();


        // retrieve if session is already present
        Document resolvedSession = retrieveSessionAndUserStateDocumentIfPresent(appName, userId, sessionId);

        if (resolvedSession == null) {
            // since no session is found creating new and also at the end only need to merge app state.
            // not the user state because the previous did not find it.

            Session newSession =
                    Session.builder(resolvedSessionId)
                            .appName(appName)
                            .userId(userId)
                            .state(initialState)
                            .events(initialEvents)
                            .lastUpdateTime(Instant.now())
                            .build();

            createNewSessionDocument(appName, userId, resolvedSessionId, newSession);

            // merge the session with the data from app state
            mergeSessionWithAppState(newSession, appName);
        }

        return inMemorySessionService.createSession(appName, userId, state, sessionId);
    }

    @Override
    public Maybe<Session> getSession(String appName, String userId, String sessionId, Optional<GetSessionConfig> config) {
        return inMemorySessionService.getSession(appName, userId, sessionId, config);
    }

    @Override
    public Single<ListSessionsResponse> listSessions(String appName, String userId) {
        return inMemorySessionService.listSessions(appName, userId);
    }

    @Override
    public Completable deleteSession(String appName, String userId, String sessionId) {
        return inMemorySessionService.deleteSession(appName, userId, sessionId);
    }

    @Override
    public Single<ListEventsResponse> listEvents(String appName, String userId, String sessionId) {
        return inMemorySessionService.listEvents(appName, userId, sessionId);
    }

    ///  --- Helpers --


    /**
     *
     * This is method searches the collection by appName -> userId -> sessionId.
     * If found returns that. Otherwise it searches by appName -> userId
     * just to check if the document at least exists.
     *
     * @param appName
     * @param userId
     * @param sessionId
     * @return Document
     */
    private Document retrieveSessionAndUserStateDocumentIfPresent(String appName, String userId, String sessionId) {
        Query query = new Query();

        Criteria criteria_one = new Criteria()
                                    .andOperator(
                                            Criteria.where("appName").is(appName),
                                            Criteria.where("userId").is(userId),
                                            Criteria.where("sessions").elemMatch(Criteria.where("sessionId").is(sessionId))
                                    );

        Criteria criteria_two = new Criteria()
                .andOperator(
                        Criteria.where("appName").is(appName),
                        Criteria.where("userId").is(userId)
                );

        Criteria criteria_root = new Criteria().orOperator(criteria_one, criteria_two);

        // check for the presence of this app and user
        List<Document> results = mongoTemplate.find(query, Document.class, this.sessionUserStateCollection);

        return !CollectionUtils.isNullOrEmpty(results) ? results.getFirst() : null;
    }

    /**
     * This method creates new entry in the collection for session and user data collection
     * @param appName
     * @param userId
     * @param resolvedSessionId
     * @param newSession
     * @return
     */

    private void createNewSessionDocument(String appName, String userId, String resolvedSessionId, Session newSession) {
        Document sessionAndUserState = new Document();
        List<Map<String, Object>> sessions = new ArrayList<>();
        Map<String, Object> sessionEntry = new HashMap<>();

        sessionEntry.put("sessionId", resolvedSessionId);
        sessionEntry.put("session", objectMapper.convertValue(newSession, Map.class));
        sessions.add(sessionEntry);
        sessionAndUserState.append("appName", appName);
        sessionAndUserState.append("userId", userId);
        sessionAndUserState.append("sessions", sessionEntry);

        // create entry
        mongoTemplate.insert(sessionAndUserState, this.sessionUserStateCollection);
    }

    private void mergeSessionWithAppState(Session session, String appName) {
        Document appStateDocument = getAppStateDocument(appName);
        if (appStateDocument == null) {
            return;
        }
        List<Map<String, Object>> appStates = objectMapper.convertValue(appStateDocument.get("states"), new TypeReference<List<Map<String, Object>>>() {});
        Map<String, Object> sessionState = session.state();

        appStates.forEach(appState -> {
            String stateKey = (String) appState.get("stateKey");
            Object stateValue = appState.get("stateValue");
            sessionState.put(State.APP_PREFIX + stateKey, stateValue);
        });
    }


    private Document getAppStateDocument(String appName) {
        Query query = new Query();
        query.addCriteria(Criteria.where("appName").is(appName));
        List<Document> appStates = mongoTemplate.find(query, Document.class, this.appStateCollection);
        return CollectionUtils.isNullOrEmpty(appStates) ? null : appStates.getFirst();
    }
}
