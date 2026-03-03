# pawn-queue vs. Other Message Queue Solutions

## Overview

pawn-queue is a pure-Python async pub/sub queue library that uses S3-compatible object storage as its message broker. This article compares it with popular alternatives to help you decide when pawn-queue is the right fit.

---

## Comparison Table

| Feature | pawn-queue | Redis Streams | RabbitMQ | AWS SQS | Kafka |
|---|---|---|---|---|---|
| External broker required | No (S3 only) | Yes (Redis) | Yes | Yes (managed) | Yes |
| Max message size | ~5 TB (S3 object limit) | 512 MB | 128 MB (default) | 256 KB | 1 MB (default) |
| Persistent storage | Yes (S3 durable) | Optional (AOF/RDB) | Yes (disk) | Yes (managed) | Yes (disk) |
| At-least-once delivery | Yes | Yes | Yes | Yes | Yes |
| Concurrent consumers | Yes (lease-based) | Yes (consumer groups) | Yes | Yes | Yes (partitions) |
| Cloud-agnostic backend | Yes | no | no | AWS only | no |
| Self-hosted option | Yes (MinIO, Ceph) | Yes | Yes | No | Yes |
| Operational overhead | Minimal | Medium | High | Low (managed) | High |
| Python async-native | Yes | Via aioredis | Via aio-pika | Via aioboto3 | Via aiokafka |

---

## Key Differentiators

### No Broker to Operate

Redis, RabbitMQ, and Kafka all require you to deploy, monitor, and maintain a dedicated broker cluster. pawn-queue uses object storage you likely already have — Hetzner Object Storage, Cloudflare R2, MinIO, or AWS S3 — as the entire backend.

### Very Large Payloads — No Workarounds Needed

This is a standout advantage of the S3-backed design.

- **AWS SQS** caps messages at **256 KB**. Larger payloads require a manual "claim-check" pattern: upload the content to S3 separately, then pass the S3 key in the SQS message.
- **Kafka** defaults to **1 MB** per message (tunable, but larger sizes degrade broker performance).
- **RabbitMQ** supports up to 128 MB by default, with significant memory pressure at scale.
- **pawn-queue** stores each message directly as an S3 object, inheriting the **~5 TB per-object limit**. You can enqueue binary files, model weights, video frames, or serialized dataframes without any claim-check indirection. Large and small messages are handled identically.

### Cost Model

Object storage is priced on capacity and request count, not on reserved compute. For workloads with bursty or low throughput, pawn-queue can be significantly cheaper than maintaining a Redis or Kafka cluster that sits mostly idle.

### Cloud-Agnostic

pawn-queue works with any S3-compatible endpoint: AWS S3, Cloudflare R2, Hetzner Object Storage, MinIO, Ceph, and more. You can switch providers by changing a single config value, with no code changes.

---

## When to Choose pawn-queue

- You already use S3-compatible storage and want to avoid adding another infrastructure component.
- Your messages can be large (documents, images, binary data, ML artifacts).
- You need a simple, operationally lean queue for Python async workloads.
- You want cloud-agnostic portability across providers.

## When to Choose an Alternative

- You need **sub-millisecond latency**: Redis Streams or in-process queues will outperform S3-backed solutions.
- You require **strict ordering guarantees** across many consumers: Kafka's partition model is purpose-built for this.
- You have **very high throughput** (millions of messages/second): dedicated brokers are more efficient at that scale.
- You need **complex routing** (exchanges, bindings, fanout): RabbitMQ's AMQP model is richer than pawn-queue's topic abstraction.

---

## Summary

pawn-queue occupies a unique niche: a zero-broker, S3-native message queue that handles arbitrarily large payloads without any workarounds. For teams already invested in object storage infrastructure, it eliminates an entire category of operational complexity while unlocking payload sizes that other queues cannot match.
