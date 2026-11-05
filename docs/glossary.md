# StreamLab Glossary

Every term used in the experiments, in plain English. New terms are added as each experiment introduces them.

## Added in Experiment 1

| Term | Meaning |
|---|---|
| **Event / message / record** | One small fact that happened, e.g. "order ORD-00001 was placed". Kafka calls it a *record*. All three words mean the same thing here. |
| **Kafka** | A program that stores events in order, on disk, and lets many programs write and read them. Think of it as a shared, append-only notebook. |
| **Broker** | One running Kafka server. It stores data and answers clients. |
| **Cluster** | A group of brokers working as one Kafka. We start with a cluster of one broker. |
| **Client** | Any program that talks to Kafka: producers and consumers. |
| **Producer** | A program that *writes* events to Kafka. Our `event-producer`. |
| **Consumer** | A program that *reads* events from Kafka. Comes in Experiment 4–5. |
| **Topic** | A named category of events, like a table name. Ours is `orders`. |
| **Partition** | A topic is split into partitions. Each partition is a separate, ordered, append-only log file. Partitions let Kafka spread load across brokers and readers. |
| **Log** | A list you can only add to at the end. Old entries are never edited. |
| **Offset** | The position number of a record *inside one partition*: 0, 1, 2, ... Each partition counts on its own. |
| **Key** | Optional label sent with a record. Kafka hashes the key to choose the partition, so equal keys always go to the same partition. |
| **Value** | The actual content of the record. Ours is an `OrderEvent` as JSON. |
| **Hash** | A function that turns any input into a number, always giving the same number for the same input. Kafka uses *murmur2*. |
| **Serializer** | Code that turns a Java object into bytes (Kafka only stores bytes). The reverse is a *deserializer*. |
| **JSON** | A text format for data: `{"orderId":"ORD-00001","quantity":1}`. |
| **Bootstrap server** | The first broker address a client connects to. From it, the client learns about every other broker. |
| **Metadata** | Information *about* the cluster: which topics exist, how many partitions, which broker leads each one. |
| **Controller** | The broker role that manages metadata and decides leaders. |
| **KRaft** | "Kafka Raft". Kafka's built-in way of storing metadata and electing the controller, using the Raft voting algorithm. It replaced ZooKeeper (a separate program older Kafka needed). |
| **Quorum** | The group of controller nodes that vote. A majority must agree for a decision. |
| **Listener** | A network "door" (host + port + protocol) a broker opens. |
| **Advertised listener** | The address the broker *tells clients* to use. Must be reachable from the client. |
| **Replication factor** | How many copies of each partition exist on different brokers. Ours is 1 (only one broker). |
| **Leader** | The broker that handles reads and writes for a partition. Covered properly in the replication experiment. |
| **Batch** | Several records sent to the broker in one network request, for speed. |
| **Docker / container** | Docker runs software in an isolated box called a *container*. We run Kafka in one, so you don't install Kafka on Windows. |
| **Docker Compose** | A YAML file describing containers to start together. |
| **Testcontainers** | A Java library that starts Docker containers from inside a test. |
| **Future (`CompletableFuture`)** | A Java object that will hold a result *later*. `send()` returns one immediately; it completes when Kafka answers. |
