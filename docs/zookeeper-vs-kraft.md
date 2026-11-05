# ZooKeeper vs KRaft — how a Kafka cluster keeps its metadata

StreamLab runs Kafka in **KRaft** mode. Older clusters, and many interview questions, still use **ZooKeeper**. This page explains what both do and why Kafka switched.

---

## 1. The problem both solve: metadata

**Metadata** is information *about* the cluster, not the messages themselves:
- which brokers are alive
- which topics exist, with how many partitions and which settings
- which broker is the **leader** of each partition (the one handling its reads and writes)
- which copies are in sync (the **ISR**, in-sync replicas)
- access rules (ACLs)

Every broker and client needs the same, up-to-date answer to these questions. One broker, the **controller**, makes the decisions, for example "broker 2 died, so broker 3 now leads partition 5". Something must store these facts reliably and decide who the controller is.

---

## 2. ZooKeeper mode (Kafka up to 3.x)

**ZooKeeper** is a separate Apache program for coordinating distributed systems: a small, very reliable store that several machines can agree on. It runs as its own cluster, called an *ensemble* (usually 3 or 5 nodes).

```
        ┌──────── ZooKeeper ensemble (3 or 5 nodes) ────────┐
        │  /brokers/ids/1,2,3      live brokers              │
        │  /brokers/topics/...     topics, partitions, ISR   │
        │  /controller             which broker is controller│
        └───────────────▲───────────────────────────────────┘
                        │ read/watch metadata
   ┌────────────┬───────┴────┬────────────┐
   │ Broker 1   │ Broker 2   │ Broker 3   │   one broker is elected controller
   │(controller)│            │            │   through ZooKeeper
   └────────────┴────────────┴────────────┘
```

How it worked:
- Each broker registered a temporary entry in ZooKeeper. If the broker died, its session expired and the entry vanished, so everyone learned it was gone.
- The first broker to write the `/controller` entry became the controller.
- The controller read all metadata from ZooKeeper, made decisions, wrote them back and pushed them to the other brokers.

**Problems**
1. **Two different systems to run.** Each needed its own installation, security, monitoring, upgrades and know-how.
2. **Slow failover.** A new controller had to load all metadata from ZooKeeper before it could act. With hundreds of thousands of partitions that took minutes, and partitions without a leader can't be written to.
3. **A scaling limit.** That reload cost capped clusters at about 200,000 partitions.
4. **Two sources of truth.** ZooKeeper and the brokers' in-memory copy could drift apart, which caused hard-to-debug bugs.

---

## 3. KRaft mode (Kafka 3.3+ production-ready, only option in 4.0+)

**KRaft** = **K**afka + **Raft**. Raft is a well-known algorithm for making several machines agree on an ordered log of decisions by majority vote.

```
   ┌──── Controller quorum (3 nodes, Raft) ────┐
   │ C1 (active) ──replicates──▶ C2, C3        │
   │ __cluster_metadata   ← an ordinary Kafka log│
   └──────────────────▲────────────────────────┘
                      │ brokers fetch the metadata log like any other log
   ┌────────────┬─────┴──────┬────────────┐
   │ Broker 1   │ Broker 2   │ Broker 3   │
   └────────────┴────────────┴────────────┘
```

How it works:
- Some Kafka nodes get the **controller** role. Together they form a **quorum** (a voting group, usually 3).
- One controller is **active**. It writes every metadata change, such as "topic created" or "leader changed", as a record in an internal Kafka log, `__cluster_metadata`.
- A change counts only once a **majority** of controllers has stored it. With 3 controllers, 1 can fail and the cluster keeps working.
- The standby controllers already hold the full log, so if the active one dies, another takes over in **seconds**. Nothing needs to be reloaded.
- Brokers fetch the metadata log the same way consumers read a topic, so they stay up to date by offset.

**Benefits**
- **One system:** Kafka only.
- **Fast failover:** seconds instead of minutes.
- **Millions of partitions** per cluster.
- **One source of truth:** the metadata log.

---

## 4. Where you see KRaft in StreamLab

| Setting in `infrastructure/docker-compose.yml` | Meaning |
|---|---|
| `KAFKA_PROCESS_ROLES: broker,controller` | This one node is both a broker and a controller ("combined mode"). Fine for learning; production keeps them separate. |
| `KAFKA_NODE_ID: 1` | Unique id of the node in the quorum. |
| `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093` | The voting group: node 1 at `kafka:9093`. Production lists 3 controllers. |
| `KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER` | Controller traffic uses its own port (9093), separate from client traffic. |
| `CLUSTER_ID` | The id the metadata storage was formatted with. |

Look at the quorum yourself:
```bat
infrastructure\kafka.bat kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
infrastructure\kafka.bat kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication
```
`LeaderId: 1` is the active controller. `describe --replication` shows how far each voter has replicated the metadata log.

---

## 5. Production layout

| | Lab (StreamLab) | Typical production |
|---|---|---|
| Controllers | 1 (combined with broker) | 3 dedicated controller nodes |
| Brokers | 1 | 3 or more |
| Survives | nothing | 1 controller and 1 broker failing at once |

Use an **odd** number of controllers. 3 tolerates 1 failure, 5 tolerates 2. 4 also tolerates only 1, because a majority of 4 is 3.

---

## 6. Timeline

| Version | Year | Status |
|---|---|---|
| Kafka 2.8 | 2021 | KRaft early access, not for production |
| Kafka 3.3 | 2022 | KRaft production-ready for new clusters |
| Kafka 3.5–3.9 | 2023–24 | ZooKeeper deprecated; migration tool from ZooKeeper to KRaft |
| Kafka 4.0 | 2025 | ZooKeeper removed completely; KRaft only |

You'll still meet ZooKeeper in older company clusters that haven't migrated yet, and in interviews.

---

## Interview questions

1. What did ZooKeeper do for Kafka?
2. Why did Kafka replace ZooKeeper with KRaft? Give three reasons.
3. What is the `__cluster_metadata` topic?
4. What is a controller quorum, and why use 3 controllers rather than 2 or 4?
5. Why is controller failover faster in KRaft?
6. What does `process.roles=broker,controller` mean, and why avoid it in production?

<details><summary>Short answers</summary>

1. Stored cluster metadata (live brokers, topics, partitions, leaders, ISR, ACLs) and elected the controller.
2. One system instead of two; failover in seconds instead of minutes; scales to millions of partitions; a single source of truth.
3. The internal Kafka log where the active controller records every metadata change; brokers and standby controllers replicate it.
4. The group of controller nodes that vote with Raft. A majority must agree. 3 survives 1 failure; 2 survives none; 4 survives only 1, the same as 3, at higher cost.
5. Standby controllers already hold the full metadata log, so a new active controller doesn't reload anything.
6. One process acts as both broker and controller. Heavy client load can then slow down metadata decisions, and losing the node loses both roles, so production separates them.
</details>
