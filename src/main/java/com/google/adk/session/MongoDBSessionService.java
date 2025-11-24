package com.google.adk.session;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.JsonBaseModel;
import com.google.adk.constants.MongoDBSessionConstants;
import com.google.adk.events.Event;
import com.google.adk.events.EventActions;
import com.google.adk.sessions.*;
import com.google.adk.utils.CollectionUtils;
import com.mongodb.BasicDBObject;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.bson.Document;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * MongoDBSessionService manages session data and state information for the Agent.
 *
 * This service requires the user to provide:
 *  1) A MongoTemplate instance
 *  2) Two collection names:
 *      - sessionUserStateCollectionName : Stores per-user sessions and user-specific states
 *      - appStateCollectionName         : Stores application-level state
 *
 * -----------------------
 * MongoDB Document Models
 * -----------------------
 *
 * Collection: session_and_user_state
 * ----------------------------------
 * Stores per-user session information and user-specific state entries.
 *
 * {
 *   "appName": "",
 *   "userId": "",
 *   "sessions": [
 *     {
 *       "sessionId": "",
 *       "session": {
 *         // Session-specific data
 *       }
 *     }
 *   ],
 *   "userStates": [
 *     {
 *       "stateKey": "",
 *       "stateValue": {
 *         // Arbitrary user-specific state object
 *       }
 *     }
 *   ]
 * }
 *
 *
 * Collection: app_state
 * ----------------------
 * Stores application-wide state that is NOT tied to any userId.
 * This collection is separate from the user session collection because
 * these states are global to the application, not user-scoped.
 *
 * {
 *   "appName": "",
 *   "appStates": [
 *     {
 *       "stateKey": "",
 *       "stateValue": {
 *         // Arbitrary app-level state object
 *       }
 *     }
 *   ]
 * }
 */

@Service
public class MongoDBSessionService  implements BaseSessionService {


    private final MongoTemplate mongoTemplate;
    private final String sessionUserStateCollection;
    private final String appStateCollection;
    private final ObjectMapper objectMapper;

    /**
     *
     * @param mongoTemplate
     * @param sessionUserStateCollectionName
     * @param appStateCollectionName
     */
    public MongoDBSessionService(MongoTemplate mongoTemplate, String sessionUserStateCollectionName, String appStateCollectionName) {
        this.mongoTemplate = mongoTemplate;
        this.sessionUserStateCollection = sessionUserStateCollectionName;
        this.appStateCollection = appStateCollectionName;
        this.objectMapper = JsonBaseModel.getMapper();
    }

    @Transactional
    @Override
    public Single<Session> createSession(String appName, String userId, @Nullable ConcurrentMap<String, Object> state, @Nullable String sessionId) {
        return Single.fromCallable(() -> {
            Objects.requireNonNull(appName, "appName cannot be null");
            Objects.requireNonNull(userId, "userId cannot be null");

            String resolvedSessionId =  Optional.ofNullable(sessionId)
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .orElseGet(() -> UUID.randomUUID().toString());

            // Ensure state map and events list are mutable for the new session
            ConcurrentMap<String, Object> initialState = (state == null) ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(state);
            List<Event> initialEvents = new ArrayList<>();

            Session newSession =
                    Session.builder(resolvedSessionId)
                            .appName(appName)
                            .userId(userId)
                            .state(initialState)
                            .events(initialEvents)
                            .lastUpdateTime(Instant.now())
                            .build();

            // Atomic Upsert operations
            Document sessionDocument = createNewSessionDocument(appName, userId, resolvedSessionId, newSession);

            mergeSessionWithUserState(newSession, sessionDocument);
            mergeSessionWithAppState(newSession, appName);

            return newSession;
        })
        .subscribeOn(Schedulers.io());
    }

    @Transactional(readOnly = true)
    @Override
    public Maybe<Session> getSession(String appName, String userId, String sessionId, Optional<GetSessionConfig> configOpt) {
        return Maybe.fromCallable(() -> {
            Objects.requireNonNull(appName, "appName cannot be null");
            Objects.requireNonNull(userId, "userId cannot be null");
            Objects.requireNonNull(sessionId, "sessionId cannot be null");
            Objects.requireNonNull(configOpt, "configOpt cannot be null");

            // get the session
            Query query = Query.query(
                    Criteria.where(MongoDBSessionConstants.APP_NAME_KEY).is(appName)
                            .and(MongoDBSessionConstants.USER_ID_KEY).is(userId)
                            .and(MongoDBSessionConstants.SESSIONS_KEY)
                            .elemMatch(
                                    Criteria.where(MongoDBSessionConstants.SESSION_ID_KEY).is(sessionId)
                            )
               );

            List<Document> sessionDocuments = mongoTemplate.find(query, Document.class, this.sessionUserStateCollection);

            if (CollectionUtils.isNullOrEmpty(sessionDocuments)) {
                return null;
            }

            List<Map<String, Object>> sessionList = objectMapper.convertValue(sessionDocuments.getFirst().get(MongoDBSessionConstants.SESSIONS_KEY), new TypeReference<List<Map<String, Object>>>() {});
            Session session = sessionList
                                .stream()
                                .filter(sessionEntryMap -> sessionId.equalsIgnoreCase((String) sessionEntryMap.get(MongoDBSessionConstants.SESSION_ID_KEY)))
                                .findAny()
                                .map(sessionMap -> objectMapper.convertValue(sessionMap.get(MongoDBSessionConstants.SESSION_KEY), Session.class))
                                .orElse(null);
            if (session == null) {
                return null;
            };

            // Apply filtering based on config directly to the mutable list in the copy
            GetSessionConfig config = configOpt.orElse(GetSessionConfig.builder().build());
            List<Event> eventsInCopy = session.events();

            config
                    .numRecentEvents()
                    .ifPresent(
                            num -> {
                                if (!eventsInCopy.isEmpty() && num < eventsInCopy.size()) {
                                    // Keep the last 'num' events by removing older ones
                                    // Create sublist view (modifications affect original list)

                                    List<Event> eventsToRemove = eventsInCopy.subList(0, eventsInCopy.size() - num);
                                    eventsToRemove.clear(); // Clear the sublist view, modifying eventsInCopy
                                }
                            });

            // Only apply timestamp filter if numRecentEvents was not applied
            if (config.numRecentEvents().isEmpty() && config.afterTimestamp().isPresent()) {
                Instant threshold = config.afterTimestamp().get();

                eventsInCopy.removeIf(
                        event -> getEventTimestampEpochSeconds(event) < threshold.getEpochSecond());
            }

            return session;
        }).subscribeOn(Schedulers.io());
    }

    // Helper to get event timestamp as epoch seconds
    private long getEventTimestampEpochSeconds(Event event) {
        return event.timestamp() / 1000L;
    }

    @Transactional(readOnly = true)
    @Override
    public Single<ListSessionsResponse> listSessions(String appName, String userId) {

        return Single.fromCallable(() -> {
            Objects.requireNonNull(appName, "appName cannot be null");
            Objects.requireNonNull(userId, "userId cannot be null");

            // Get the session list from the app and user search
            List<Session> sessions = getAllSessions(appName, userId);

            return ListSessionsResponse.builder().sessions(sessions).build();

        }).subscribeOn(Schedulers.io());

    }

    private List<Session> getAllSessions(String appName, String userId) {
        // Fetch Query
        Query query = Query.query(Criteria.where(MongoDBSessionConstants.APP_NAME_KEY).is(appName).and(MongoDBSessionConstants.USER_ID_KEY).is(userId));

        List<Document> results = mongoTemplate.find(query, Document.class);

        if (CollectionUtils.isNullOrEmpty(results)) {
            return new ArrayList<>();
        }

        // Get the sessions Map
        List<Map<String, Object>> sessionsMap = objectMapper.convertValue(results.getFirst(), new TypeReference<List<Map<String, Object>>>() {});

        return sessionsMap
                .stream()
                .map(sessionMapObject -> objectMapper.convertValue(sessionMapObject.get(MongoDBSessionConstants.SESSION_ID_KEY), Session.class))
                .toList();
    }

    @Transactional
    @Override
    public Completable deleteSession(String appName, String userId, String sessionId) {
        return Completable.fromRunnable(() -> {
            Query query = Query.query(Criteria.where(MongoDBSessionConstants.APP_NAME_KEY).is(appName).and(MongoDBSessionConstants.USER_ID_KEY).is(userId));
            Update update = new Update().pull(MongoDBSessionConstants.SESSIONS_KEY, new BasicDBObject(MongoDBSessionConstants.SESSION_ID_KEY, sessionId));
            mongoTemplate.updateFirst(query, update, sessionUserStateCollection);
        })
        .subscribeOn(Schedulers.io()); // Offload to IO thread
    }

    @Override
    public Single<ListEventsResponse> listEvents(String appName, String userId, String sessionId) {
        return Single.fromCallable(() -> {
            Objects.requireNonNull(appName, "appName cannot be null");
            Objects.requireNonNull(userId, "userId cannot be null");
            Objects.requireNonNull(sessionId, "sessionId cannot be null");

            return getSession(appName, userId, sessionId, Optional.empty())
                    .map(session -> ListEventsResponse.builder().events(session.events()).build())
                    .map(Single::just);

        }).flatMap(Maybe::toSingle)
                .flatMap(listEventsResponseSingle -> listEventsResponseSingle)
                .subscribeOn(Schedulers.io());
    }


    @Transactional
    @Override
    public Single<Event> appendEvent(Session session, Event event) {
        return Single.fromCallable(() -> {

            Objects.requireNonNull(session, "session cannot be null");
            Objects.requireNonNull(event, "event cannot be null");
            Objects.requireNonNull(session.appName(), "session.appName cannot be null");
            Objects.requireNonNull(session.userId(), "session.userId cannot be null");
            Objects.requireNonNull(session.id(), "session.id cannot be null");

            // Temporary holder for user state and app state
            List<Map<String, Object>> userStates = new ArrayList<>();
            List<Map<String, Object>> appStates = new ArrayList<>();

            // Original Number Of States Count in Session
            long originalStateCount = session.state().size();

            // --- Update User/App State
            EventActions actions = event.actions();
            if (actions != null) {
                Map<String, Object> stateDelta = actions.stateDelta();
                if (stateDelta != null && !stateDelta.isEmpty()) {
                    stateDelta.forEach(
                            (key, value) -> {
                                if (key.startsWith(State.APP_PREFIX)) {
                                    String appStateKey = key.substring(State.APP_PREFIX.length());
                                    appStates.add(Map.of(MongoDBSessionConstants.STATE_KEY_KEY, appStateKey, MongoDBSessionConstants.STATE_VALUE_KEY, value));
                                } else if (key.startsWith(State.USER_PREFIX)) {
                                    String userStateKey = key.substring(State.USER_PREFIX.length());
                                    userStates.add(Map.of(MongoDBSessionConstants.STATE_KEY_KEY, userStateKey, MongoDBSessionConstants.STATE_VALUE_KEY, value));
                                } else {
                                    session.state().put(key, value);
                                }
                            });
                }
            }

            BaseSessionService.super.appendEvent(session, event);
            session.lastUpdateTime(getInstantFromEvent(event));

            // Multi document update
            if (!userStates.isEmpty() || originalStateCount < session.state().size()) {
                addEventToSessionDocument(session, userStates);
            }
            if (!appStates.isEmpty()) {
                bulkUpdateAppStateDocument(session, appStates);
            }

            // Update session and user state
            mergeSessionWithAppState(session, session.appName());
            mergeSessionWithUserState(session, userStates);

            return event;
        }).subscribeOn(Schedulers.io()); // Offload to IO thread;
    }

    private void bulkUpdateAppStateDocument(Session session, List<Map<String, Object>> appStates) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, Document.class, this.appStateCollection);

        // Get the stateKeys of the app to remove old state objects in the DB
        Object [] appStateKeyValues = appStates
                .stream()
                .map(appState ->  new BasicDBObject(MongoDBSessionConstants.STATE_KEY_KEY, appState.get(MongoDBSessionConstants.STATE_KEY_KEY)))
                .toArray();

        // Query
        Query appMatchingQuery = Query.query(Criteria.where(MongoDBSessionConstants.APP_NAME_KEY).is(session.appName()));

        // Delete duplicates
        Update pullOld = new Update()
                                    .pullAll(MongoDBSessionConstants.APP_STATES_KEY, appStateKeyValues)
                                    .setOnInsert(MongoDBSessionConstants.APP_NAME_KEY, session.appName()); // Run only if doc is created;
        // Add new states
        Update addNew = new Update()
                                    .push(MongoDBSessionConstants.APP_STATES_KEY).each(appStates);

        bulkOperations.updateMulti(
                List.of(
                        Pair.of(appMatchingQuery, pullOld),
                        Pair.of(appMatchingQuery, addNew)
                )
        );
        bulkOperations.execute();
    }

    private void addEventToSessionDocument(Session session, List<Map<String, Object>> userStates) {

        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, Document.class, this.sessionUserStateCollection);

        // Session Query
        Query sessionQuery = Query.query(
                Criteria.where(MongoDBSessionConstants.APP_NAME_KEY).is(session.appName())
                        .and(MongoDBSessionConstants.USER_ID_KEY).is(session.userId())
                        .and(MongoDBSessionConstants.SESSIONS_KEY)
                        .elemMatch(Criteria.where(MongoDBSessionConstants.SESSION_ID_KEY).is(session.id()))
        );

        Map<String, Object> sessionEntry = new HashMap<>();
        sessionEntry.put(MongoDBSessionConstants.SESSION_ID_KEY, session.id());
        sessionEntry.put(MongoDBSessionConstants.SESSION_KEY, objectMapper.convertValue(session, new TypeReference<Map<String, Object>>() {}));

        // Update the session on the matching sessionId
        Update sessionUpdate =  new Update().set("sessions.$", sessionEntry);

        // User states query
        Query userStateQuery = Query.query(Criteria.where(MongoDBSessionConstants.APP_NAME_KEY).is(session.appName()).and(MongoDBSessionConstants.USER_ID_KEY).is(session.userId()));
        Update userStateUpdate = new Update().push(MongoDBSessionConstants.USER_STATES_KEY).each(userStates);

        bulkOperations.updateMulti(
                List.of(
                        Pair.of(sessionQuery, sessionUpdate),
                        Pair.of(userStateQuery, userStateUpdate)
                )
        );
        bulkOperations.execute();
    }

    private Instant getInstantFromEvent(Event event) {
        double epochSeconds = getEventTimestampEpochSeconds(event);
        long seconds = (long) epochSeconds;
        long nanos = (long) ((epochSeconds % 1.0) * 1_000_000_000L);
        return Instant.ofEpochSecond(seconds, nanos);
    }
    
    private Document createNewSessionDocument(String appName, String userId, String resolvedSessionId, Session newSession) {
        Map<String, Object> sessionEntry = new HashMap<>();

        sessionEntry.put(MongoDBSessionConstants.SESSION_ID_KEY, resolvedSessionId);
        sessionEntry.put(MongoDBSessionConstants.SESSION_KEY, objectMapper.convertValue(newSession, Map.class));

        Query query = Query.query(Criteria.where(MongoDBSessionConstants.APP_NAME_KEY).is(appName).and(MongoDBSessionConstants.USER_ID_KEY).is(userId));

        Update update = new Update()
                .setOnInsert(MongoDBSessionConstants.APP_NAME_KEY, appName) // Run only if doc is created
                .setOnInsert(MongoDBSessionConstants.USER_ID_KEY, userId)
                .setOnInsert(MongoDBSessionConstants.APP_STATES_KEY, new ArrayList<>())
                .push(MongoDBSessionConstants.SESSIONS_KEY, sessionEntry); // Run always

        // Create entry (Upsert)
        return mongoTemplate.findAndModify(
                query,
                update,
                FindAndModifyOptions.options().returnNew(true).upsert(true),
                Document.class,
                this.sessionUserStateCollection
        );
    }

    private void mergeSessionWithAppState(Session session, String appName) {
        Document appStateDocument = getAppStateDocument(appName);
        if (appStateDocument == null) {
            return;
        }
        List<Map<String, Object>> appStates = objectMapper.convertValue(appStateDocument.get(MongoDBSessionConstants.APP_STATES_KEY), new TypeReference<List<Map<String, Object>>>() {});
        Map<String, Object> sessionState = session.state();

        // merge states
        appStates.forEach(appState -> {
            String stateKey = (String) appState.get(MongoDBSessionConstants.STATE_KEY_KEY);
            Object stateValue = appState.get(MongoDBSessionConstants.STATE_VALUE_KEY);
            sessionState.put(State.APP_PREFIX + stateKey, stateValue);
        });
    }

    private void mergeSessionWithUserState(Session session, List<Map<String, Object>> userStates) {
        Map<String, Object> sessionState = session.state();
        // Merge states
        userStates.forEach(userStatesMap -> {
                sessionState.put(State.USER_PREFIX + userStatesMap.get(MongoDBSessionConstants.STATE_KEY_KEY), userStatesMap.get(MongoDBSessionConstants.STATE_VALUE_KEY));
        });
    }

    private void mergeSessionWithUserState(Session session, Document resolvedSession) {
        List<Map<String, Object>> userStates = objectMapper.convertValue(resolvedSession.get(MongoDBSessionConstants.USER_STATES_KEY), new TypeReference<List<Map<String, Object>>>() {});

        if (CollectionUtils.isNullOrEmpty(userStates)) {
            return;
        }
        Map<String, Object> sessionState = session.state();
        // Merge states
        userStates.forEach(userState -> {
            String stateKey = (String) userState.get(MongoDBSessionConstants.STATE_KEY_KEY);
            Object stateValue = userState.get(MongoDBSessionConstants.STATE_VALUE_KEY);
            sessionState.put(State.USER_PREFIX + stateKey, stateValue);
        });
    }

    private Document getAppStateDocument(String appName) {
        Query query = new Query();
        query.addCriteria(Criteria.where(MongoDBSessionConstants.APP_NAME_KEY).is(appName));
        List<Document> appStates = mongoTemplate.find(query, Document.class, this.appStateCollection);
        return CollectionUtils.isNullOrEmpty(appStates) ? null : appStates.getFirst();
    }
}
