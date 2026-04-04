# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
./mvnw clean package          # Build and package
./mvnw spring-boot:run        # Run the application
./mvnw clean test             # Run all tests
./mvnw test -Dtest=ClassName  # Run a single test class
./mvnw spring-boot:build-image # Build OCI container image
```

The app requires a Kafka broker running at `localhost:9092` before starting. Topics are auto-created by `KafkaConfig` on startup (3 partitions, replication factor 1).

## Architecture

Java 21, Spring Boot 4.0.5.

**Request flow:**
```
POST /api/event/{topic}?partition={optional}&key={optional}
  →  EventController  →  UserEventSenderImpl  →  KafkaTemplate  →  Kafka broker
```

**Consumption flow:**
```
Topic: user-event
  → Group "fanout"   → EventListener.handle()   [no filter, no error handler]
  → Group "fanout2"  → EventListener.handle2()  [balance >= 10,000 filter; throws on "error" in username]
        ↓ (after 3 retries, 2s backoff)
  Topic: user-event.DLT

Topic: another-user-event
  → Group "fanout"   → EventListener.handle()   [no filter, no error handler]
```

**Key configuration in `KafkaConfig`:**
- Two listener container factories:
  - `kafkaListenerContainerFactory` (default) — used by `handle()`; no message filter, no error handler
  - `filterKafkaListenerContainerFactory` — used by `handle2()`; applies `balance >= 10,000` filter, wires `DefaultErrorHandler` with fixed backoff (3 retries, 2s), and routes failures to `{topic}.DLT`
- `ConsumerFactory` deserializes to `User`; trusted package `com.vasyl.practice.*`; group ID hardcoded to `"test-group-id"`; `AUTO_OFFSET_RESET = earliest`
- `KafkaTemplate` serializes values as JSON via Jackson; producer key type is `String`
- `ack-mode=manual_immediate` + `enable-auto-commit=false` — consumers must explicitly acknowledge

**`EventSender<T>` interface** is the generic contract for producers; `UserEventSenderImpl` is the only implementation and is wired into the controller.

**`EventListener.handle2()`** intentionally throws a `RuntimeException` when `username` contains `"error"` to exercise the DLT/retry path.
