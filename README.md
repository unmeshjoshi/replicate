This is a basic framework to quickly build and test replication algorithms.
It has basic mechanism to establish request-response and message passing communication
between replicas. It also has test utilities to quickly form a cluster of replicas,
introduce network failures and assert on the state of the replicas.
This repo has example code for the distributed system workshop.
# Basic design
This framework allows implementing replication algorithms quickly and write 
JUnit tests. The framework also has some basic ways to introduce faults like
process crash, network disconnections, network delays and clock skews.
Here I outline the steps required to implement a replication algorithm. 
There are more complete examples in paxos, multipaxos and vsr folders.

## [Replica](src/main/java/replicate/common/Replica.java) class
The Replica class implements all the basic building blocks required for 
implementing a networked service.
- It listens on the provided IP address and port.
    - Two IP address and ports need to be provided. One to listen for client 
      requests and one for peer requests.
    - It maintains a single threaded executor. Each request is executed in 
      within this single thread of execution. This allows doing state 
      changes while handling the requests without any hassle. This is an 
      example implementation of <a href="https://martinfowler.com/articles/patterns-of-distributed-systems/singular-update-queue.html">Singular Update Queue</a> pattern.
    - It provides a basic implementation of <a href="https://martinfowler.com/articles/patterns-of-distributed-systems/request-waiting-list.html">Request Waiting List</a> pattern, which tracks client requests 
      and responds once the replication criteria in fulfilled.
    - It provides serialization and deserialization of the messages defined 
      as plain old Java classes. As of now it implements JSON serialization.

These building blocks are good enough to implement and test any networked 
service.

## Writing JUnit tests
There are utilities provided to create multiple Replica instances.
Because these replica instances are created as Java objects, 
it is very easy to inspect its state.
[QuorumKVStoreTest](src/test/java/replicate/quorum/QuorumKVStoreTest.java) is a 
good example of how to write the test.

## Introducing failures
The [Replica](src/main/java/replicate/common/Replica.java) class allows
introducing network failures to other nodes. It has utility methods 
to either drop messages or delay messages to other Replica instances.
[QuorumKVStoreTest](src/test/java/replicate/quorum/QuorumKVStoreTest.java) has
examples of how to test by introducing network failures.

# Available Replication Algorithms
There are many example replication mechanisms coded in this repository

1. [Basic Read/Write Quorum](src/main/java/replicate/quorum/QuorumKVStore.java)
2. [Quorum Consensus](src/main/java/replicate/quorumconsensus/QuorumConsensus.java)
2. [Single value paxos](src/main/java/replicate/paxos/SingleValuePaxos.java)
3. [Paxos based key-value store](src/main/java/replicate/paxoskv/PaxosKVStore.java)
3. [Paxos based replicated log](src/main/java/replicate/paxoslog/PaxosLog.java)
4. [MultiPaxos](src/main/java/replicate/mpaxos/MultiPaxos.java)
4. [View Stamped Replication](src/main/java/replicate/vsr/ViewStampedReplication.java)
  