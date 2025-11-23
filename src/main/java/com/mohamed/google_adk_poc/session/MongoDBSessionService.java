package com.mohamed.google_adk_poc.session;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.JsonBaseModel;
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
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

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
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        if ((Boolean)event.partial().orElse(false)) {
            return Single.just(event);
        } else {
            EventActions actions = event.actions();
            if (actions != null) {
                ConcurrentMap<String, Object> stateDelta = actions.stateDelta();
                if (stateDelta != null && !stateDelta.isEmpty()) {
                    ConcurrentMap<String, Object> sessionState = session.state();
                    if (sessionState != null) {
                        stateDelta.forEach((key, value) -> {
                            if (!key.startsWith("temp:")) {
                                if (value == State.REMOVED) {
                                    sessionState.remove(key);
                                } else {
                                    sessionState.put(key, value);
                                }
                            }

                        });
                    }
                }
            }

            List<Event> sessionEvents = session.events();
            if (sessionEvents != null) {
                sessionEvents.add(event);
            }

            // update the db
            Query query = Query.query(Criteria.where("appName").is(session.appName())
                    .and("userId").is(session.userId())
                    .and("sessions.sessionId").is(session.id()));

            Map<String, Object> sessionEntry = new HashMap<>();
            sessionEntry.put("sessionId", session.id());
            sessionEntry.put("session", objectMapper.convertValue(session, new TypeReference<Map<String, Object>>() {}));

            Update update = new Update().set("sessions.$", sessionEntry);
            // Execute Atomic Update
            mongoTemplate.updateFirst(query, update, sessionUserStateCollection);
            return Single.just(event);
        }
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
