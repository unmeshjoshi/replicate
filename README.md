# Replicate

**Note:** This framework was built while working on the book [Patterns of Distributed Systems](https://www.informit.com/store/patterns-of-distributed-systems-9780138221980).
I use this to teach replication techniques in the workshops that I conduct.

```java
/*                                 message1     +--------+
                                +-------------->+        |
                                |               | node2  |
                                |    +-message2-+        |
                                |    |          |        |
                             +--+----v+         +--------+
 +------+   request-response |        |
 |      |                    |node1   |
 |client| <--------------->  |        |
 |      |                    |        |
 +------+                    +-+----+-+
                               |    ^            +---------+
                               |    |            |         |
                               |    +--message4--+ node3   |
                               |                 |         |
                               +--message3------->         |
                                                 +---------+
*/                              
```
## Overview
This is a basic framework for quickly building and testing replication algorithms. It doesn't require any additional setup (e.g., Docker) for setting up a cluster and allows writing simple JUnit tests for testing replication mechanisms. The framework was created to learn and teach various distributed system techniques, enabling the testing of working code while exploring distributed systems concepts. It provides mechanisms for establishing message-passing communication between replicas and test utilities for quickly forming a cluster of replicas, introducing network failures, and asserting the state of the replicas. This repository also contains example code for a distributed system workshop.

## Basic Design

This framework allows you to implement replication algorithms quickly and write JUnit tests. It also offers basic ways to introduce faults like process crashes, network disconnections, network delays, and clock skews.

### Replica Class

The [Replica](src/main/java/replicate/common/Replica.java) class 
implements essential building blocks for a networked 
service, including:

- Listening on provided IP addresses and ports.
- Managing a single-threaded executor for request handling, Implementing the 
  [Singular Update Queue](https://martinfowler.com/articles/patterns-of-distributed-systems/singular-update-queue.html) pattern.
- Providing a basic implementation of the [Request Waiting List](https://martinfowler.com/articles/patterns-of-distributed-systems/request-waiting-list.html) pattern.
- Supporting serialization and deserialization of messages (currently using JSON).

These building blocks are sufficient for implementing and testing any networked service.

### Writing JUnit Tests

Utilities are provided to create multiple `Replica` instances. These instances, being Java objects, are easy to inspect and test. Check out [QuorumKVStoreTest](src/test/java/replicate/quorum/QuorumKVStoreTest.java) for an example of how to write tests.

### Introducing Failures

The `Replica` class allows you to introduce network failures to other nodes, with utility methods for dropping or delaying messages. Examples of testing with introduced network failures can be found in [QuorumKVStoreTest](src/test/java/replicate/quorum/QuorumKVStoreTest.java).

## Available Replication Algorithms

This repository contains example replication mechanisms, including:

1. [Basic Read/Write Quorum](src/main/java/replicate/quorum/QuorumKVStore.java)
2. [Quorum Consensus](src/main/java/replicate/quorumconsensus/QuorumConsensus.java)
3. [Single-value Paxos](src/main/java/replicate/paxos/SingleValuePaxos.java)
4. [Paxos-based Key-Value Store](src/main/java/replicate/paxoskv/PaxosKVStore.java)
5. [Paxos-based Replicated Log](src/main/java/replicate/paxoslog/PaxosLog.java)
6. [MultiPaxos](src/main/java/replicate/multipaxos/MultiPaxos.java)
7. [View Stamped Replication](src/main/java/replicate/vsr/ViewStampedReplication.java)

Explore these algorithms to understand and experiment with different replication techniques.

