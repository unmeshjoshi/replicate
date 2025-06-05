# 2-Day Distributed Systems Workshop Agenda

> **Workshop Format**: Each teaching block is 45 minutes 
> **Breaks**: 10–15 minutes between sessions  
> **Daily Structure**: Day 1: ~3.75 hours teaching (5 sessions), Day 2: ~3 hours teaching (4 sessions) 

---

## **Workshop Setup** (5 minutes)
**Python Performance Analysis Tools Setup:**
```bash
cd src/main/python
python3 -m venv venv
source venv/bin/activate
pip install numpy matplotlib scipy

# Test installation
python queuing_theory.py
```

---

## Day 1: Foundations & Basic Patterns

### **Session 1** (45 min) **Why Distribute?**
- **Learning Goals:**
  - Resource ceilings and physical limits
  - Little's Law and performance modeling
  - Motivation for distributed patterns
- **Hands-on Lab:** Run provided disk-perf test; capture own numbers
- **Performance Analysis:**
  ```bash
  # Demonstrate system performance limits with queuing theory
  cd src/main/python
  source venv/bin/activate
  
  # Show performance degradation as load increases
  python queuing_theory.py
  
  # Visualize the performance curves
  python queuing_theory_visualization.py
  ```
  **Key Insights:** 
  - System performance degrades dramatically near 100% utilization
  - At 90% load: 100ms latency (manageable)
  - At 99% load: 1000ms latency (problematic)
  - Beyond 100%: System collapse
- **Connection:** "This is WHY we need distributed systems - single machines hit performance walls!"
- **Break:** 10 minutes

### **Session 2** (45 min) **Why Patterns? & Partial Failure Mindset**
- **Learning Goals:**
  - Understanding the need for patterns
  - Pattern-based thinking in distributed systems
  - Probability of failure at scale and network partitions
  - Process pauses and their impact
- **Hands-on Lab:** 
  - Overview of patterns available in the framework
  - Walkthrough of the 'replicate' framework with fault injection
- **Failure Probability Analysis:**
  ```bash
  # Calculate realistic failure probabilities
  python failure_probability.py
  ```
  **Key Insights:**
  - Script calculates "N or more failures" probability using binomial distribution
  - Patterns help us handle these inevitable failures systematically
- **Connection:** "Patterns solve recurring problems - especially failure handling!"
- **Reference:** `src/main/java/replicate/common/` and `src/test/java/replicate/common/`
- **Break:** 10 minutes

### **Session 3** (45 min) **Write-Ahead Log Pattern**
- **Learning Goals:**
  - Append-only discipline for durability
  - Recovery mechanisms and replay
  - WAL as foundation for other patterns
- **Hands-on Lab:** Execute and walkthrough `DurableKVStoreTest` for persistent key-value store
- **Connection:** "WAL ensures we can recover from the failures we just discussed!"
- **Reference:** `src/test/java/replicate/wal/DurableKVStoreTest.java`
- **Break:** 15 minutes

### **Session 4** (45 min) **Code Walkthrough and Core Patterns**
- **Learning Goals:**
  - Request-waiting list pattern for async operations
  - Singular update queue for thread safety
  - Network messaging foundations
  - Building blocks for distributed protocols
- **Hands-on Lab:** 
  - Code walkthrough: `RequestWaitingList` and `SingularUpdateQueue` implementations
  - Understand how async requests are tracked and managed
  - See how single-threaded execution prevents race conditions
- **Reference:** 
  - `src/main/java/replicate/common/RequestWaitingList.java`
  - `src/main/java/replicate/common/SingularUpdateQueue.java`
  - `src/main/java/replicate/net/` - Network communication layer
- **Connection:** "These patterns are the foundation for quorum-based systems and consensus algorithms!"
- **Break:** 10 minutes

### **Session 5** (45 min) **Replication & Majority Quorum**
- **Learning Goals:**
  - Write vs read quorums trade-offs
  - Quorum intersection properties
  - Universal Scalability Law curve analysis
- **Hands-on Lab:** Modify `QuorumKVStoreTest`: pass for 5-node/3-node clusters
  - **Prerequisite:** Understanding of `RequestWaitingList` from Session 4 (used in quorum coordination)
- **Scalability Analysis:**
  ```bash
  # Analyze how performance scales with cluster size
  python universal_scalability_law_improved.py
  ```
  **Key Visualizations Generated:**
  1. **Distributed System Performance Scaling** - Shows how coordination overhead affects scaling
  2. **Business Impact Metrics** - Throughput (req/s) and Response Time (ms) scaling
  3. **Consensus Algorithm Comparison** - Performance differences between Paxos, RAFT, etc.
  
  **Key Insights:**
  - Adding more nodes doesn't always improve performance
  - Coordination overhead increases with cluster size
  - Optimal cluster sizes depend on algorithm choice
  - Well-designed systems scale better than legacy systems
- **Connection:** "This shows the trade-offs in quorum-based replication!"
- **Reference:** `src/test/java/replicate/quorum/QuorumKVStoreTest.java`
- **End of Day 1**

---

## Day 2: Consensus Algorithms & Advanced Patterns

### **Session 6** (45 min) **Why Simple Replication Fails**
- **Learning Goals:**
  - Two-phase commit pitfalls
  - Recovery ambiguity problems
  - The need for consensus algorithms
- **Hands-on Lab:** Step through `DeferredCommitmentTest` and `RecoverableDeferredCommitmentTest`; 
- **Realistic System Behavior Analysis:**
  ```bash
  # Show how systems degrade under stress (unlike theoretical models)
  python realistic_system_performance.py
  ```

### **Session 7** (45 min) **Single-Value Paxos**
- **Learning Goals:**
  - Prepare/Accept phases explained
  - Recovery with generation numbers
  - Safety and liveness properties
- **Hands-on Lab:** Work with generation voting mechanism using existing Paxos tests
- **Reference:** `src/test/java/replicate/paxos/` and `src/test/java/replicate/generationvoting/`
- **Break:** 10 minutes

### **Session 8** (45 min) **From Paxos to Multi-Paxos**
- **Learning Goals:**
  - Replicated log concept and implementation
  - High-water mark for safe execution
  - Heartbeats and failure detection
- **Hands-on Lab:** Extend log to multi-slot using Multi-Paxos and Paxos Log implementations
- **Reference:** `src/test/java/replicate/multipaxos/` and `src/test/java/replicate/paxoslog/`
- **Break:** 15 minutes

### **Session 9** (45 min) **RAFT vs Multi-Paxos in Practice**
- **Learning Goals:**
  - Implementation optimizations comparison
  - Idempotent receiver pattern
  - Production considerations and future directions
- **Hands-on Lab:** Compare RAFT & Multi-Paxos implementations; annotate pros/cons
  ```
  **Discussion Points:**
  - **RAFT vs Multi-Paxos**: Which scales better and why?
  - **Optimal cluster sizes**: 3, 5, 7, or more nodes?
  - **Production trade-offs**: Performance vs complexity vs reliability

- **End of Day 2**

