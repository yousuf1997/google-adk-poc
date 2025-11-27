
---

# 📚 Google ADK Session Management: Custom MongoDB Service

This project implements a custom **MongoDB-backed session and state management service** for the Google Agent Development Kit (ADK) in Java environments (like Spring Boot). It serves as a robust, persistent alternative to the official in-memory solution.

## 💡 Core Principle: Implementation and Behavior Alignment with ADK

This service is engineered for maximum compatibility and familiarity with the Google ADK's native design:

1.  **API and Behavior:** The service's public interface (`BaseSessionService`) and interaction logic are strictly modeled after the conceptual flow of the official Google ADK's `InMemorySessionService` API.
2.  **Code Borrowing:** Specific utility or logic code, where persistence is not a factor (e.g., event manipulation or timestamp handling), has been **directly borrowed** from the original ADK `InMemorySessionService` implementation. This ensures consistency in how in-session data structures are managed.

This adherence to the ADK model allows developers to easily transition to this persistent MongoDB solution.

***

## ✨ Key Features

* **Reactive and Transactional:** Built using **RxJava 3** (`Single`, `Maybe`, `Completable`) for asynchronous I/O and Spring's **`@Transactional`** annotation for atomic database operations.
* **Scoped State Management:** Supports three distinct state scopes, which are automatically handled upon event ingestion:
    * **Session State:** Data specific to the current interaction.
    * **User State:** Data persistent across all sessions for a specific user (prefixed with `user:`).
    * **Application State:** Global data persistent across all users and sessions (prefixed with `app:`).
* **Efficient Event Logging:** Allows appending new `Event` objects to a session.
* **Session Retrieval Filters:** Supports filtering the returned list of events by **timestamp** or keeping only the **`N` most recent events** (`GetSessionConfig`).

***

## 💾 MongoDB Data Model Structure

The service utilizes two separate, configurable MongoDB collections to manage separation between user-specific and global data.

### 1. `session_and_user_state` Collection

Stores data tied to a specific user, indexed by `appName` and `userId`.

| Field Name | Description | Scope |
| :--- | :--- | :--- |
| `appName` | The Agent/Application identifier. | Application |
| `userId` | The unique identifier for the user. | User |
| `sessions` | An array containing all sessions for this user. | User |
| `sessions[].session` | The full session object, including events and session-scoped state. | Session |
| `userStates` | Key-value pairs of state data persistent for the user across sessions. | User |

### 2. `app_state` Collection

Stores global, application-wide data that is independent of any specific user.

| Field Name | Description | Scope |
| :--- | :--- | :--- |
| `appName` | The Agent/Application identifier (Document ID). | Application |
| `appStates` | Key-value pairs of state data global to the entire application. | Application |

***

## 🛠️ State Management Conventions

When an event is processed via `appendEvent`, the service inspects the event's `actions.stateDelta` map. The presence of a prefix determines the persistence location, aligning with the ADK's expected state mutation behavior.

| Prefix | State Scope | Persistence Location |
| :--- | :--- | :--- |
| `user:` | **User-Persistent** | `session_and_user_state.userStates` (External document update) |
| `app:` | **Application-Global** | `app_state.appStates` (External bulk update) |
| **None** | **Session-Scoped** | Updated directly within the session object's state map |

***

## 💻 Service API Highlights (Methods)

The following methods implement the core `BaseSessionService` interface:

* **`createSession`**: Initializes a new session, performing an atomic upsert operation on the user document.
* **`getSession`**: Retrieves a specific session document, merging any existing User and App state into the returned `Session` object.
* **`listSessions`**: Retrieves a list of all active sessions for a given user.
* **`deleteSession`**: Removes a session entry from the user's document.
* **`listEvents`**: Retrieves the event history for a specific session.
* **`appendEvent`**: The core write operation. It atomically appends the event and performs concurrent updates to Session, User, and App states based on the event's `stateDelta`.