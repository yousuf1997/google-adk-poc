package com.mohamed.google_adk_poc.session;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.JsonBaseModel;
import com.google.adk.events.Event;
import com.google.adk.events.EventActions;
import com.google.adk.sessions.*;
import com.google.adk.utils.CollectionUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.client.result.UpdateResult;
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

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

final public class MongoDBSessionService  implements BaseSessionService {

    /***
     * Document structure
     *  - SESSION_USER_STATE -
     * {
     *     "appName" : "",
     *     "userId" : "",
     *     "sessions" : [
     *       {
     *           "sessionId" : "",
     *           "session" : {
     *
     *           }
     *       }
     *     ],
     *     "userStates" : [
     *        {
     *            "stateKey": "",
     *           "stateValue" : {}
     *        }
     *     ]
     * }
     * -- APP STATE
     * {
     *     "appName" : "",
     *     "appStates" : [
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
//        .flatMap(session -> inMemorySessionService.createSession(appName, userId, state, sessionId))
        .subscribeOn(Schedulers.io());
    }

    @Override
    public Maybe<Session> getSession(String appName, String userId, String sessionId, Optional<GetSessionConfig> configOpt) {
        return Maybe.fromCallable(() -> {
            Objects.requireNonNull(appName, "appName cannot be null");
            Objects.requireNonNull(userId, "userId cannot be null");
            Objects.requireNonNull(sessionId, "sessionId cannot be null");
            Objects.requireNonNull(configOpt, "configOpt cannot be null");

            // get the session
            Query query = Query.query(
                    Criteria.where("appName").is(appName)
                            .and("userId").is(userId)
                            .and("sessions")
                            .elemMatch(
                                    Criteria.where("sessionId").is(sessionId)
                            )
               );

            List<Document> sessionDocuments = mongoTemplate.find(query, Document.class, this.sessionUserStateCollection);

            if (CollectionUtils.isNullOrEmpty(sessionDocuments)) {
                return null;
            }

            List<Map<String, Object>> sessionList = objectMapper.convertValue(sessionDocuments.getFirst().get("sessions"), new TypeReference<List<Map<String, Object>>>() {});
            Session session = sessionList
                                .stream()
                                .filter(sessionEntryMap -> sessionId.equalsIgnoreCase((String) sessionEntryMap.get("sessionId")))
                                .findAny()
                                .map(sessionMap -> objectMapper.convertValue(sessionMap.get("session"), Session.class))
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

    @Override
    public Single<ListSessionsResponse> listSessions(String appName, String userId) {
        return inMemorySessionService.listSessions(appName, userId);
    }

    @Override
    public Completable deleteSession(String appName, String userId, String sessionId) {
        return Completable.fromRunnable(() -> {
            Query query = Query.query(Criteria.where("appName").is(appName).and("userId").is(userId));
            // ATOMIC DELETE: "Pull" (remove) the element from 'sessions' array where 'sessionId' matches
            Update update = new Update().pull("sessions", new BasicDBObject("sessionId", sessionId));
            mongoTemplate.updateFirst(query, update, sessionUserStateCollection);
        })
        .subscribeOn(Schedulers.io()); // Offload to IO thread
//        // update the document
//        return inMemorySessionService.deleteSession(appName, userId, sessionId).mergeWith(dbDelete);
    }

    @Override
    public Single<ListEventsResponse> listEvents(String appName, String userId, String sessionId) {
        return inMemorySessionService.listEvents(appName, userId, sessionId);
    }


    @Override
    public Single<Event> appendEvent(Session session, Event event) {
        return Single.fromCallable(() -> {

            Objects.requireNonNull(session, "session cannot be null");
            Objects.requireNonNull(event, "event cannot be null");
            Objects.requireNonNull(session.appName(), "session.appName cannot be null");
            Objects.requireNonNull(session.userId(), "session.userId cannot be null");
            Objects.requireNonNull(session.id(), "session.id cannot be null");

            String appName = session.appName();
            String userId = session.userId();
            String sessionId = session.id();

            // Temporary holder for user state
            List<Map<String, Object>> userStates = new ArrayList<>();
            List<Map<String, Object>> appStates = new ArrayList<>();

            // --- Update User/App State (Same as before) ---
            EventActions actions = event.actions();
            if (actions != null) {
                Map<String, Object> stateDelta = actions.stateDelta();
                if (stateDelta != null && !stateDelta.isEmpty()) {
                    stateDelta.forEach(
                            (key, value) -> {
                                if (key.startsWith(State.APP_PREFIX)) {
                                    String appStateKey = key.substring(State.APP_PREFIX.length());
                                    appStates.add(Map.of("stateKey", appStateKey, "stateValue", value));

                                } else if (key.startsWith(State.USER_PREFIX)) {
                                    String userStateKey = key.substring(State.USER_PREFIX.length());
                                    userStates.add(Map.of("stateKey", userStateKey, "stateValue", value));
                                } else {
                                    session.state().put(key, value);
                                }
                            });
                }
            }

            BaseSessionService.super.appendEvent(session, event);
            session.lastUpdateTime(getInstantFromEvent(event));

            // TODO : Following things can be made transactional since it involves updating two separate collections.

            // update the db session data
            Query query = Query.query(Criteria.where("appName").is(session.appName())
                    .and("userId").is(session.userId())
                    .and("sessions.sessionId").is(session.id()));
            Map<String, Object> sessionEntry = new HashMap<>();
            sessionEntry.put("sessionId", session.id());
            sessionEntry.put("session", objectMapper.convertValue(session, new TypeReference<Map<String, Object>>() {}));
            Update update = new Update().push("userStates").each(userStates).set("sessions.$", sessionEntry);
            // Execute Atomic Update
            mongoTemplate.updateFirst(query, update, sessionUserStateCollection);

            if (!appStates.isEmpty()) {

                BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, Document.class, this.appStateCollection);

                // get the stateKeys of the app
                Map<String, Object> appStateKeyValues = appStates
                        .stream()
                        .flatMap(stringObjectMap -> stringObjectMap.keySet().stream())
                        .collect(Collectors.toMap(o -> {
                            return "stateKey";
                        }, o -> {
                            return o;
                        }));

                // delete duplicates
                Query appMatchingQuery = Query.query(Criteria.where("appName").is(session.appName()));
                Update pullOld = new Update().pull("userStates", new BasicDBObject("stateKey", appStateKeyValues));
                // add new states
                Update addNew = new Update().push("userStates").each(appStates);

                // Execute Atomic Update
                bulkOperations.updateOne(
                        List.of(
                                Pair.of(appMatchingQuery, pullOld),
                                Pair.of(appMatchingQuery, addNew)
                        )
                );
            }

            // update session and user state
            mergeSessionWithAppState(session, session.appName());
            mergeSessionWithUserState(session, userStates);

            return event;
        });
    }

    private void updateAppState(String appName, String appStateKey, Object value) {
        Query query = Query.query(Criteria.where("appName").is(appName));
        Map<String, Object> appStateEntry = new HashMap<>();

        appStateEntry.put("stateKey", appStateKey);
        appStateEntry.put("stateValue", value);

        Update update = new Update()
                .setOnInsert("appName", appName) // Run only if doc is created
                .push("appStates", appStateEntry); // Run always

        mongoTemplate.findAndModify(
                query,
                update,
                FindAndModifyOptions.options().returnNew(true).upsert(true),
                Document.class,
                this.appStateCollection
        );
    }

    private Instant getInstantFromEvent(Event event) {
        double epochSeconds = getEventTimestampEpochSeconds(event);
        long seconds = (long) epochSeconds;
        long nanos = (long) ((epochSeconds % 1.0) * 1_000_000_000L);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    ///  --- Helpers --



    /**
     * This method creates new entry in the collection for session and user data collection.
     * This is Atomic operation.
     * @param appName
     * @param userId
     * @param resolvedSessionId
     * @param newSession
     * @return
     */

    private Document createNewSessionDocument(String appName, String userId, String resolvedSessionId, Session newSession) {
        Map<String, Object> sessionEntry = new HashMap<>();

        sessionEntry.put("sessionId", resolvedSessionId);
        sessionEntry.put("session", objectMapper.convertValue(newSession, Map.class));

        Query query = Query.query(Criteria.where("appName").is(appName).and("userId").is(userId));

        Update update = new Update()
                .setOnInsert("appName", appName) // Run only if doc is created
                .setOnInsert("userId", userId) // Run only if doc is created
                .setOnInsert("userStates", new ArrayList<>()) // Run only if doc is created
                .push("sessions", sessionEntry); // Run always

        // create entry
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
        List<Map<String, Object>> appStates = objectMapper.convertValue(appStateDocument.get("appStates"), new TypeReference<List<Map<String, Object>>>() {});
        Map<String, Object> sessionState = session.state();

        // merge states
        appStates.forEach(appState -> {
            String stateKey = (String) appState.get("stateKey");
            Object stateValue = appState.get("stateValue");
            sessionState.put(State.APP_PREFIX + stateKey, stateValue);
        });
    }

    private void mergeSessionWithUserState(Session session, List<Map<String, Object>> userStates) {
        Map<String, Object> sessionState = session.state();
        // merge states
        userStates.forEach(userStatesMap -> {
            for (Map.Entry<String, Object> entry : userStatesMap.entrySet())
                sessionState.put(State.USER_PREFIX + entry.getKey(), entry.getValue());
        });
    }

    private void mergeSessionWithUserState(Session session, Document resolvedSession) {
        List<Map<String, Object>> userStates = objectMapper.convertValue(resolvedSession.get("userStates"), new TypeReference<List<Map<String, Object>>>() {});

        if (CollectionUtils.isNullOrEmpty(userStates)) {
            // todo handle this
            return;
        }
        Map<String, Object> sessionState = session.state();
        // merge states
        userStates.forEach(userState -> {
            String stateKey = (String) userState.get("stateKey");
            Object stateValue = userState.get("stateValue");
            sessionState.put(State.USER_PREFIX + stateKey, stateValue);
        });
    }



    private Document getAppStateDocument(String appName) {
        Query query = new Query();
        query.addCriteria(Criteria.where("appName").is(appName));
        List<Document> appStates = mongoTemplate.find(query, Document.class, this.appStateCollection);
        return CollectionUtils.isNullOrEmpty(appStates) ? null : appStates.getFirst();
    }
}
