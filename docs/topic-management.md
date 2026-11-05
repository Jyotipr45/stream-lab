# Topic Management — how product companies handle many topics

Our lab has one topic. A real company has hundreds or thousands: orders, payments, deliveries, sign-ups, plus Kafka's own internal topics. This page explains the habits that keep that manageable, and where each one appears in StreamLab.

New words are defined in [glossary.md](glossary.md).

---

## 1. A naming convention

A topic name should tell you, without opening anything, **who owns it, what it holds and which format version it carries**.

```
<domain>.<entity>.<event>.<version>

orders.order.placed.v1            the orders team; an order was placed; format v1
payments.payment.failed.v1
delivery.rider.location-updated.v1
users.profile.v1                  a "state" topic (latest profile per user) has no event part
```

| Part | Meaning | Rule |
|---|---|---|
| domain | the business area / team that owns it | lower case |
| entity | the thing the event is about | singular |
| event | what happened, past tense | `placed`, `cancelled`, `location-updated` |
| version | message format version | bump only for a **breaking** change (a field removed or its type changed) |

**Rules we follow**
- Lower case only. Dots between parts, hyphens inside a part.
- Never use both `.` and `_` in topic names on one cluster. Kafka turns `.` into `_` in metric names, so `a.b` and `a_b` collide.
- Environment (`dev`, `prod`) is not in the name. Companies use a **separate cluster per environment**, so the same code runs unchanged everywhere.
- One topic per **kind** of event, never one per customer or per order. `orders-customer-123` is wrong. Use one topic and put the customer id in the **key**.

**In StreamLab:** `orders.order.placed.v1`, set in `event-producer/src/main/resources/application.yml`. The integration test `topicFollowsNamingConvention()` fails the build if someone breaks the pattern.

---

## 2. Every topic has an owner

The service that **produces** an event owns its topic:
- it decides partitions, retention and the message format
- it creates the topic
- other teams only **consume** it, and ask the owner for changes

So if `payments.payment.failed.v1` misbehaves, everybody knows which team to call.

**In StreamLab:** `event-producer` owns `orders.order.placed.v1`, so the topic is declared in `KafkaTopicConfig` inside the producer. The consumer (Experiment 4+) will never create it.

---

## 3. Topics are created from reviewed code, never by hand

Running `kafka-topics.sh --create` on production is like editing a production database table by hand: nobody reviews it, and nobody can rebuild it later. Instead:

```
developer edits a topic file ──▶ pull request ──▶ review ──▶ merge ──▶ pipeline creates/updates the topic
```

The topic file is usually Terraform (a tool that creates infrastructure from config files) or a YAML catalogue read by an internal tool:

```yaml
- name: orders.order.placed.v1
  owner: orders-team
  partitions: 12
  replicationFactor: 3
  config:
    retention.ms: 604800000        # 7 days
    min.insync.replicas: 2
```

The broker runs with `auto.create.topics.enable=false`, so a typo in a topic name fails loudly instead of silently creating a new topic.

**In StreamLab:** the small version of this. The topic's shape lives in `application.yml` (reviewed in git), and `KafkaTopicConfig` turns it into a `NewTopic` bean that Spring's `KafkaAdmin` creates at startup. `docker-compose.yml` sets `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`.

---

## 4. Settings depend on what the topic is for

| Topic type | Example | Typical settings | StreamLab |
|---|---|---|---|
| **Event stream**: facts that happened | `orders.order.placed.v1` | `cleanup.policy=delete`, `retention.ms` = days | ✅ 7 days |
| **State / changelog**: latest value per key | `users.profile.v1` | `cleanup.policy=compact` (keep only the newest record per key) | Experiment on log compaction |
| **Dead Letter Topic**: messages that failed processing | `orders.order.placed.v1.dlt` | long retention, so failures can be inspected and replayed | Experiment 9 |
| **Internal**: created by Kafka itself | `__consumer_offsets`, `__cluster_metadata`, Streams `*-changelog` | managed by Kafka; don't touch | Experiments 4, 13 |

**Retention** is how long Kafka keeps records before deleting them. It is a per-topic setting, so each topic can choose: clickstream data 1 day, orders 7 days, audit events a year.

---

## 5. Agreeing on the message format: Schema Registry

**Problem:** the orders team renames `amount` to `totalAmount`. Five other teams that read the topic break at once, in production.

**Solution:** a **Schema Registry**, a separate service that stores the agreed format (*schema*) of each topic's messages, usually in **Avro** or **Protobuf** (compact binary formats with a strict structure).
1. The producer registers the schema before writing.
2. The registry checks every new version against the old one with a **compatibility rule**. For example, "you may add an optional field, you may not delete one".
3. A breaking change is **rejected** at build or deploy time, before anything breaks.
4. A real breaking change gets a **new topic version** (`v2`). Both run side by side while consumers migrate.

**In StreamLab:** we use plain JSON to keep things simple. `spring.json.add.type.headers: false` keeps the JSON free of Java class names, so any language can read it. A Schema Registry is a possible later add-on.

---

## 6. Who may read and write: ACLs

**ACLs** (access control lists) are broker rules such as:

```
User:payment-service   ALLOW  WRITE  Topic:payments.*      (prefixed)
User:fraud-service     ALLOW  READ   Topic:orders.order.placed.v1
```

Each application connects with its own identity (a certificate or username), so a bug in one service can't write garbage into another team's topic.

**In StreamLab:** none. The lab broker is open on `localhost`. Never run a real cluster like that.

---

## 7. Seeing everything: catalogue and monitoring

Companies keep a **topic catalogue**: one page listing every topic, its owner, schema, partitions, retention, producers and consumers. Tools include Kafka UI, AKHQ, Confluent Control Center or an internal portal.

They monitor and alert on:
- **Consumer lag**: how far behind the newest record a consumer group is. Rising lag means the consumers are too slow or stuck. (Experiment 5+)
- **Under-replicated partitions**: copies that have fallen behind the leader. (Replication experiment)
- **Disk usage and bytes in/out per topic**: finds hot topics and wrong retention.

---

## 8. Choosing the number of partitions

- Partitions = the **maximum number of consumers in one group that can work in parallel**.
- You can increase partitions later, never decrease them. Increasing moves keys to other partitions (`hash % 4 ≠ hash % 3`), which breaks per-key ordering across old and new data.
- So choose up front: *(peak messages per second) ÷ (messages per second one consumer can handle)*, then add headroom. 6, 12 or 24 are common starting values; thousands on one topic is rarely right.

**In StreamLab:** 3, so you can watch three consumers share the work in Experiment 5.

---

## Checklist: adding a new topic

- [ ] Name follows `<domain>.<entity>.<event>.<version>`
- [ ] Owner (producing service) is clear
- [ ] Declared in code/config and reviewed, never created by hand
- [ ] Partitions sized for peak load, with headroom
- [ ] Replication factor 3 and `min.insync.replicas=2` in production (1 in this lab)
- [ ] Retention or compaction chosen for the topic's purpose
- [ ] Message key chosen by "which events must stay in order?"
- [ ] Schema registered (when a registry is used)
- [ ] ACLs: who writes, who reads
- [ ] A Dead Letter Topic, if consumers can fail on bad messages

---

## Interview questions

1. How would you name topics in a company with 50 teams? Why put a version in the name?
2. Who should create a topic, the producer team or the consumer team? Why?
3. Why disable `auto.create.topics.enable` in production?
4. An event topic and a state topic: how do their retention settings differ?
5. A producer renamed a JSON field and consumers broke. How do you stop that happening again?
6. Why is one topic per customer a bad design?
7. How do you decide the number of partitions, and why is changing it later risky?
