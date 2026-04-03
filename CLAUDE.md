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

The app requires a Kafka broker running at `localhost:9092` before starting. Topics are auto-created by `KafkaConfig` on startup.

## Architecture

**Request flow:**
```
POST /api/event/{topic}  →  EventController  →  UserEventSenderImpl  →  KafkaTemplate  →  Kafka broker
```

**Consumption flow:**
```
Topic: user-event
  → Group "fanout"   → EventListener.handle()   [balance >= 10,000 filter]
  → Group "fanout2"  → EventListener.handle2()  [balance >= 10,000 filter; throws on "error" in username]
        ↓ (after 3 retries, 2s backoff)
  Topic: user-event.DLT
```

**Key configuration in `KafkaConfig`:**
- `filterKafkaListenerContainerFactory` — used by all listeners; applies the balance filter, wires `DefaultErrorHandler` with fixed backoff (3 retries, 2s), and routes failures to `{topic}.DLT`
- `ConsumerFactory` deserializes to `User`; trusted package scope is `com.vasyl.practice.*`
- `KafkaTemplate` serializes values as JSON via Jackson
- `ack-mode=manual_immediate` + `enable-auto-commit=false` — consumers must explicitly acknowledge

**`EventSender<T>` interface** is the generic contract for producers; `UserEventSenderImpl` is the only implementation and is wired into the controller.

**`EventListener.handle2()`** intentionally throws a `RuntimeException` when `username` contains `"error"` to exercise the DLT/retry path.
