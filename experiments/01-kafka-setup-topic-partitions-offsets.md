# Experiment 1 — Kafka in KRaft mode, a 3-partition topic, partitions and offsets

**Goal:** start Kafka with Docker, create the `orders` topic with 3 partitions, publish 10 order events, and see exactly which partition and offset each one got.

Unknown word? See [docs/glossary.md](../docs/glossary.md).

---

## 0. Prerequisites

| Tool | Check | Notes |
|---|---|---|
| Docker Desktop | `docker version` | Must be running. |
| JDK 17 | `java -version` | Not strictly required: Gradle downloads a JDK 17 if missing. |
| Nothing else | | No Kafka install, no Gradle install. The project ships the Gradle Wrapper (`gradlew`). |

---

## 1. Concepts, in simple English

### Broker and cluster
- **What:** a broker is one Kafka server. A cluster is several brokers acting as one system.
- **Why it exists:** one machine has limited disk, CPU and network, and can fail. Many brokers share the load and keep copies of data.
- **How it works inside:** each broker stores some partitions on its disk. Clients connect to any broker (the *bootstrap server*), download the cluster *metadata*, and then talk directly to the broker that leads the partition they need.

### KRaft
- **What:** the way a Kafka cluster keeps its metadata (topic list, partition count, who leads what) and chooses a controller.
- **Why it exists:** older Kafka needed a second system, ZooKeeper, for this. Two systems meant double operations work and slower recovery. KRaft puts it inside Kafka. Kafka 4.0 removed ZooKeeper completely.
- **How it works inside:** some nodes have the *controller* role. They form a *quorum* and use the Raft algorithm: one active controller writes every metadata change to an internal log called `__cluster_metadata`, and the other controllers copy it. A change counts only when a majority has it. Brokers read that log to learn the current state.
- In our compose file one node has both roles (`KAFKA_PROCESS_ROLES: broker,controller`). Fine for learning, never for production.

### Topic and partition
- **What:** a topic is a named stream of events (`orders`). It is split into partitions: separate, ordered, append-only logs.
- **Why it exists:** one log can only be written to and read from so fast, and must fit on one disk. Splitting into partitions lets several brokers and several consumers work in parallel.
- **How it works inside:** each partition is a folder on the broker's disk (`orders-0`, `orders-1`, `orders-2`) containing *segment* files. New records are appended at the end. Nothing is edited in place.

### Offset
- **What:** the position of a record in its partition: 0, 1, 2, ...
- **Why it exists:** a reader needs to say "I've read up to here". An offset is that bookmark.
- **How it works inside:** each partition has its own counter. Partition 0 and partition 1 both have an offset 0. So a record is identified by **topic + partition + offset**, never by offset alone. **Ordering is guaranteed only inside one partition**, not across the topic.

### Producer, key and value
- **What:** the producer writes records. Each record has an optional **key** and a **value**.
- **Why keys exist:** to keep related events together and in order. All events for one order (or one customer) should land in one partition, so they are read in the order they happened.
- **How the partition is chosen (Kafka 3.x default partitioner):**
  - Key present: `partition = positive(murmur2(keyBytes)) % numberOfPartitions`. Same key → same partition, always (as long as the partition count doesn't change).
  - Key is `null`: the *sticky partitioner* sends a whole batch to one partition, then switches. So 10 key-less messages sent quickly often all land in **one** partition. That surprises many people; we test it in Experiment 3.

### Serialization
- **What:** turning a Java object into bytes and back.
- **Why it exists:** Kafka doesn't know Java classes. It stores and sends bytes only.
- **How it works here:** `StringSerializer` turns the key `"ORD-00001"` into UTF-8 bytes. Spring Kafka's `JsonSerializer` turns `OrderEvent` into JSON bytes using Jackson.

---

## 2. What we built

```
stream-lab/
├── settings.gradle, build.gradle        Gradle multi-project build
├── gradlew, gradlew.bat, gradle/        Gradle Wrapper 8.11.1
├── run.bat / run.sh                     start Kafka + run an experiment
├── test.bat / test.sh                   run all tests
├── infrastructure/
│   ├── docker-compose.yml               Kafka 3.9.1, KRaft, single node
│   └── kafka.bat / kafka.sh             shortcut for Kafka CLI tools in the container
├── event-producer/
│   └── src/main/java/com/jyoti/streamlab/producer/
│       ├── EventProducerApplication.java
│       ├── model/OrderEvent.java                  the message value (record)
│       ├── config/OrdersTopicProperties.java      topic name/partitions/replicas from YAML
│       ├── config/KafkaTopicConfig.java           NewTopic bean → topic created at startup
│       ├── config/GeneratorConfig.java            Clock + seeded Random
│       ├── generator/OrderEventGenerator.java     fake orders, keys ORD-00001, ORD-00002 ...
│       ├── publisher/OrderEventPublisher.java     KafkaTemplate.send(topic, key, value)
│       └── experiment/Experiment01Runner.java     sends N events, prints partition + offset
├── experiments/                         one guide per experiment (this file)
└── docs/glossary.md
```

**Why this shape:** each class has one job. The generator doesn't know Kafka exists, the publisher doesn't know events are fake, and the runner only orchestrates and prints. Later experiments reuse the generator and publisher and add new runners.

**Flow:**
```
Experiment01Runner ──next()──▶ OrderEventGenerator
        │
        └──publish(event)──▶ OrderEventPublisher ──send("orders", orderId, event)──▶ KafkaTemplate
                                                                                        │
                       serialize key+value → choose partition (murmur2) → add to batch ─┘
                                                                                        │
                                        background sender thread ──batch──▶ broker (localhost:9092)
                                                                                        │
           future completes with RecordMetadata(partition, offset) ◀──── broker appends and replies
```

---

## 3. Important configuration explained

### `infrastructure/docker-compose.yml`

| Setting | Value | Why |
|---|---|---|
| `image` | `apache/kafka:3.9.1` | Official Apache image. 3.9 matches the `kafka-clients` 3.9 that Spring Boot 3.5 brings. |
| `KAFKA_NODE_ID` | `1` | Every node in a KRaft cluster needs a unique number. |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | Combined mode: one node does both jobs. |
| `CLUSTER_ID` | fixed string | The storage is formatted with this id. Fixed so restarts recognise their data. |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka:9093` | The voters: node 1 at `kafka:9093`. With 3 controllers you'd list 3. |
| `KAFKA_LISTENERS` | three ports | 9093 controller voting, 19092 container-to-container, 9092 for your laptop. |
| `KAFKA_ADVERTISED_LISTENERS` | `localhost:9092` for host | Broker tells your Java app "reach me at localhost:9092". If this were wrong, the first connection would work and every later one would fail — a classic Kafka-in-Docker bug. |
| `*_REPLICATION_FACTOR: 1` | | Internal topics default to 3 copies. With 1 broker that's impossible, so we lower it. |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` | A typo in a topic name fails loudly instead of silently creating a new topic. |
| `healthcheck` | `kafka-broker-api-versions.sh` | Docker marks the container *healthy* only when the broker answers. `run.bat` waits for that. |

### `event-producer/src/main/resources/application.yml`

| Setting | Meaning |
|---|---|
| `bootstrap-servers: localhost:9092` | First broker to contact. |
| `key-serializer: StringSerializer` | Key `String` → bytes. |
| `value-serializer: JsonSerializer` | `OrderEvent` → JSON bytes. |
| `acks: all` | Broker answers "OK" only after all in-sync copies have the record. Full lesson in the producer experiments. |
| `enable.idempotence: true` | Broker drops duplicates caused by producer retries. Already the default since Kafka 3.0; written to be visible. |
| `linger.ms: 5` | Wait up to 5 ms to fill a batch. More batching = fewer requests. |
| `max.block.ms: 10000` | If Kafka is unreachable, fail after 10 s instead of the default 60 s. Faster feedback while learning. |
| `spring.json.add.type.headers: false` | Don't attach the Java class name as a header, so the JSON is readable by any language. |
| `streamlab.orders-topic.*` | Name, 3 partitions, 1 replica. Read by `OrdersTopicProperties`. |
| `streamlab.experiment: 1` | Which runner to execute. |

### `build.gradle`
Spring Boot **3.5.16** (Java 17+), managing spring-kafka 3.3.x, kafka-clients 3.9.x, JUnit 5, Mockito and Testcontainers. The **toolchain** forces Java 17 for compiling and running, so you can't use Java 21 APIs by accident.

---

## 4. Run it

### Windows (cmd / PowerShell, from `D:\04_Projects\Practice_book\stream-lab`)
```bat
run.bat 1
```
### macOS / Linux / Git Bash
```bash
./run.sh 1
```

What `run` does: `docker compose up -d` → waits for *healthy* → `gradlew :event-producer:bootRun --args="--streamlab.experiment=1"`.

The first run downloads the Kafka image (~400 MB) and Gradle dependencies, so it takes a few minutes.

---

## 5. Expected output

Keys and the partition count are fixed, and Kafka's hash is deterministic, so on a **fresh topic** you should see exactly this (time stamps differ):

```
=== Experiment 1: sending 10 messages to topic 'orders' (3 partitions) ===
key=ORD-00001  customer=CUST-4  ->  partition=1  offset=0
key=ORD-00002  customer=CUST-1  ->  partition=0  offset=0
key=ORD-00003  customer=CUST-5  ->  partition=2  offset=0
key=ORD-00004  customer=CUST-3  ->  partition=2  offset=1
key=ORD-00005  customer=CUST-2  ->  partition=1  offset=1
key=ORD-00006  customer=CUST-1  ->  partition=0  offset=1
key=ORD-00007  customer=CUST-1  ->  partition=2  offset=2
key=ORD-00008  customer=CUST-4  ->  partition=1  offset=2
key=ORD-00009  customer=CUST-1  ->  partition=1  offset=3
key=ORD-00010  customer=CUST-2  ->  partition=0  offset=2
=== Messages per partition: {0=3, 1=4, 2=3} ===
```

**Read it carefully:**
- Offsets restart at 0 in every partition. `offset=2` appears three times — in three different partitions.
- The split is 3/4/3, not exactly even. Hashing spreads keys *roughly* evenly; with only 10 keys, small imbalance is normal.

**Run it a second time** (`run.bat 1` again). The app restarts its counter, so it sends the same keys ORD-00001…00010 again:
```
key=ORD-00001  customer=CUST-4  ->  partition=1  offset=4
key=ORD-00002  customer=CUST-1  ->  partition=0  offset=3
...
=== Messages per partition: {0=3, 1=4, 2=3} ===
```
Same keys → same partitions as before. Offsets **continue** (partition 1 is now at 4..7). Kafka never reuses or overwrites an offset.

---

## 6. Inspect with Kafka CLI tools

All commands run *inside* the container. On Windows use `infrastructure\kafka.bat`, otherwise `infrastructure/kafka.sh`. The long form is `docker exec -it streamlab-kafka /opt/kafka/bin/<tool> ...`.

**a) Is KRaft working? Who is the controller?**
```bat
infrastructure\kafka.bat kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
```
Expected (abridged):
```
ClusterId:              5L6g3nShT-eMCtK--X86sw
LeaderId:               1
LeaderEpoch:            1
CurrentVoters:          [{"id": 1, ...}]
```
`LeaderId: 1` = node 1 is the active controller.

**b) Describe the topic**
```bat
infrastructure\kafka.bat kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
```
Expected:
```
Topic: orders  TopicId: ...  PartitionCount: 3  ReplicationFactor: 1  Configs:
    Topic: orders  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
    Topic: orders  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
    Topic: orders  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```
`Leader: 1` — broker 1 handles this partition. `Isr` = in-sync replicas (copies that are up to date). Both explained fully in the replication experiment.

**c) Latest offset per partition** (after one run)
```bat
infrastructure\kafka.bat kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
```
Expected:
```
orders:0:3
orders:1:4
orders:2:3
```
Format is `topic:partition:nextOffset` — the offset the *next* record will get. It equals the number of records in that partition (as long as nothing was deleted).

**d) Read the messages back, with partition, offset and key**
```bat
infrastructure\kafka.bat kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --property print.partition=true --property print.offset=true --property print.key=true --timeout-ms 5000
```
Expected (order between partitions may vary; inside one partition it never does):
```
Partition:0	Offset:0	ORD-00002	{"orderId":"ORD-00002","customerId":"CUST-1","product":"Mouse","quantity":1,"amount":800.00,"createdAt":"..."}
Partition:0	Offset:1	ORD-00006	{...}
...
Processed a total of 10 messages
```

**e) Create a topic by hand** (what `KafkaTopicConfig` does for you)
```bat
infrastructure\kafka.bat kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders-cli --partitions 3 --replication-factor 1
infrastructure\kafka.bat kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**f) Look at the partition folders on disk**
```bat
docker exec streamlab-kafka sh -c "ls /tmp/kraft-combined-logs | grep orders"
```
Expected: `orders-0  orders-1  orders-2`. Each partition really is a directory of log files.

**Stop Kafka**
```bat
docker compose -f infrastructure\docker-compose.yml down
```
`down` deletes the container, so all topics and messages are gone. Use `stop` / `start` to keep them.

---

## 7. Failure scenarios — try each one

| # | Do this | What happens | Lesson |
|---|---|---|---|
| 1 | Stop Kafka (`docker stop streamlab-kafka`), then `gradlew.bat :event-producer:bootRun` | KafkaAdmin can't create the topic; `send()` waits `max.block.ms` (10 s) and fails with `TimeoutException: ... not present in metadata` | A producer can't send without metadata. In production, alert on this instead of retrying silently. |
| 2 | `kafka-topics.sh ... --create --topic bad --partitions 3 --replication-factor 2` | `InvalidReplicationFactorException: Replication factor: 2 larger than available brokers: 1` | Copies must live on different brokers. |
| 3 | `kafka-topics.sh ... --alter --topic orders --partitions 2` | `InvalidPartitionsException: Topic currently has 3 partitions, which is higher than the requested 2` | Partitions can only be *increased*. Removing one would delete data. |
| 4 | `kafka-topics.sh ... --alter --topic orders --partitions 4`, then `run.bat 1` again | Some keys now land in a **different** partition than before (`hash % 4` ≠ `hash % 3`). | Adding partitions breaks "same key → same partition" for existing keys. Choose the partition count up front. |
| 5 | Change `bootstrap-servers` to `localhost:9999` and run | Connection refused warnings, then timeout after 10 s | The bootstrap address must be reachable. |
| 6 | In `docker-compose.yml`, change the host advertised listener to `PLAINTEXT_HOST://kafka:9092`, recreate, run | First connection works, then `UnknownHostException: kafka` / timeouts | Clients use the *advertised* address, not the one you typed. |

After 3 and 4, reset with `docker compose ... down` then `up -d`.

---

## 8. Production use and trade-offs

- **How many partitions?** Partitions = maximum parallel consumers in one group. More partitions → more throughput, but more open files, longer leader elections, more memory in clients. A common start: estimate peak throughput ÷ throughput one consumer can handle, round up, add headroom. Tens per topic is normal; thousands per topic is rarely right.
- **Choosing the key:** use the thing whose events must stay in order — orderId for order lifecycle, customerId for per-customer fraud checks. A bad key (e.g. country, where 80 % of traffic is one country) creates a *hot partition*: one partition overloaded while others idle.
- **Ordering:** guaranteed per partition only. Need global order? Use 1 partition and accept no parallelism — usually the wrong trade.
- **Never in production:** single node, replication factor 1, auto-create on. Use 3+ brokers, 3 controllers, replication factor 3, `min.insync.replicas=2`.
- **KRaft vs ZooKeeper:** new clusters are KRaft-only (Kafka 4.0 removed ZooKeeper). Fewer moving parts, faster controller failover, supports far more partitions.

---

## 9. Interview questions

1. What is the difference between a topic and a partition? Why does Kafka have partitions at all?
2. Is an offset unique across a topic? How do you uniquely identify a record?
3. How does the producer choose the partition when a key is given? When the key is null?
4. Two events for the same order must be processed in order. How do you guarantee that?
5. What happens to key → partition mapping if you increase partitions from 3 to 4? Can you decrease them?
6. What is KRaft? What problem did it solve compared to ZooKeeper?
7. What is the difference between `listeners` and `advertised.listeners`? Describe a bug caused by getting it wrong.
8. Why can't you create a topic with replication factor 3 on a single broker?
9. `KafkaTemplate.send()` returned — is the message stored? When do you actually know?
10. What is a hot partition and how do you avoid it?

<details><summary>Short answers</summary>

1. Topic = named stream; partition = one ordered log inside it. Partitions allow parallelism and spreading data over brokers.
2. No. Each partition counts from 0. Unique id = topic + partition + offset.
3. murmur2 hash of key bytes, made positive, modulo partition count. Null key: sticky partitioner fills a batch for one partition, then switches.
4. Use the orderId as key → same partition → ordered. (Plus idempotent producer so retries don't reorder.)
5. Many keys move to a different partition; old events stay where they were, so per-key order across old and new data breaks. Decreasing is not allowed.
6. Kafka's built-in Raft-based metadata quorum. Removes the separate ZooKeeper system, simplifies operations, faster failover, more partitions.
7. `listeners` = where the broker binds; `advertised.listeners` = address given to clients. Wrong advertised address → first bootstrap works, then clients can't reach the broker.
8. Each copy must be on a different broker; one broker can hold only one copy.
9. No. `send()` only buffers. You know when the future completes successfully (with `acks=all`, after in-sync replicas wrote it).
10. One partition gets most traffic because of a skewed key. Pick higher-cardinality keys, or add a suffix/salt when order isn't required.
</details>

---

## 10. Validate before moving on

Tick all of these, then tell me to start Experiment 2.

- [ ] `test.bat` passes (7 tests: 3 generator, 1 publisher with Mockito, 3 Kafka integration with Testcontainers — integration needs Docker running).
- [ ] `run.bat 1` prints the table in section 5.
- [ ] `kafka-get-offsets.sh` shows `orders:0:3  orders:1:4  orders:2:3` after one run.
- [ ] Second run: same partitions, offsets continued.
- [ ] You tried at least failure scenarios 1, 3 and 4.
- [ ] You can answer interview questions 2, 3 and 5 without looking.
