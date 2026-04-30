# Architectural Analysis: What Are We Missing?

> **Status: HISTORICAL.** This document is the original architectural gap analysis written *before* the Blueprint. Every gap below has been resolved and is now tracked in the canonical spec:
>
> - **Conceptual gaps (this doc)** → `BLUEPRINT.md §2` — mapped to phases.
> - **Stress-test invariants** (claim-lease, OOM, GC, clock skew, shutdown, producer idempotency, FIFO, migrations, payload contract) → `BLUEPRINT.md §7` — 10 invariants with implementation rules.
>
> Keep this doc for posterity; do not edit it further. New issues go into the Blueprint.

---

We have a very solid foundation. The core problem, the solution, the concurrency model (`SKIP LOCKED`), and the ecosystem positioning are all correct.

However, moving from a "theoretical concept" to a **production-grade open-source library**, I have analyzed the architecture and identified 7 critical missing pieces we need to address before we start coding.

## 1. Garbage Collection (The Table Will Explode)
**The Gap:** We mentioned marking messages as `PROCESSED`. If a system processes 100 events a second, that's 8.6 million rows a day. The outbox table will eventually bring the database to its knees due to table size and index bloat.
**The Fix:** We need a built-in Garbage Collection strategy.
- **Option A:** Delete the row immediately upon successful publish instead of marking it `PROCESSED`. (Saves space, reduces DB load, but loses history).
- **Option B:** A background cron job within the library that deletes `PROCESSED` rows older than `X` days (e.g., 7 days) to maintain an audit trail without crashing the DB.

## 2. API Design for ORMs (Prisma vs. TypeORM)
**The Gap:** NestJS developers rarely use raw SQL. They use TypeORM, Prisma, Sequelize, or MikroORM. These ORMs handle transactions completely differently.
- TypeORM uses an `EntityManager` or `QueryRunner` that you pass around.
- Prisma uses a callback function `prisma.$transaction(async (tx) => { ... })`.
**The Fix:** The library must have specific adapters or a very clever API design so it feels native to the ORM the user has chosen. We cannot force them to use raw SQL for the outbox if they are using Prisma for their business logic.

## 3. Exponential Backoff & Retry State
**The Gap:** We mentioned a Dead Letter Queue (DLQ) if the broker is down. But if Kafka goes down for 5 minutes, our worker shouldn't hammer Kafka with 10,000 requests per second failing instantly in an infinite loop.
**The Fix:** The outbox table needs two more columns: `retry_count` and `next_retry_at`. If a publish fails, we increment `retry_count` and set `next_retry_at` using an exponential backoff formula (e.g., retry in 2s, then 4s, then 8s) until it hits `maxRetries` and goes to the DLQ.

## 4. Postgres LISTEN/NOTIFY (The Latency Killer)
**The Gap:** We rely entirely on polling. If we poll every 5 seconds, an event might sit there for 4.9 seconds before being picked up. For real-time microservices, this latency is unacceptable.
**The Fix:** Since Postgres is our primary target, we should leverage its native `LISTEN`/`NOTIFY` feature. When a row is inserted, Postgres can send a lightweight notification to our worker to wake it up instantly. This changes our system from "pure pull" to "push-optimized with pull-fallback", reducing latency from seconds to milliseconds.

## 5. CloudEvents Standard Specification
**The Gap:** What does the actual JSON payload look like when it hits Kafka? If every developer invents their own schema, interoperability across microservices becomes a nightmare.
**The Fix:** We should natively support (or strongly default to) the **CloudEvents** specification (a CNCF standard). Every event should automatically get standard headers (`id`, `source`, `specversion`, `type`, `time`, `data`).

## 6. Embedded vs. Standalone Worker Deployment
**The Gap:** Where does the Relay Worker actually run?
**The Fix:** We need to support two deployment modes:
1. **Embedded Mode:** Runs inside the NestJS app (e.g., using `@nestjs/schedule`). Great for startups and small apps. Low infrastructure overhead.
2. **Standalone Mode:** A CLI command (e.g., `npx outbox-relay`) that runs as a separate Docker container. Essential for high-scale enterprise apps so the polling and publishing don't steal CPU cycles from the main API server.

## 7. Crucial Alerting Metrics (The "Golden Signals")
**The Gap:** We mentioned Prometheus metrics, but what exactly are we measuring? Without the right metrics, the outbox could fail silently.
**The Fix:** We must explicitly track and expose these critical metrics:
- `outbox_pending_depth`: How many messages are waiting? (If this spikes, the worker is stuck or dead - CRITICAL alert).
- `outbox_publish_latency_seconds`: Time difference between `created_at` and successful publish.
- `outbox_dlq_depth`: Number of dead-lettered messages requiring manual intervention.
