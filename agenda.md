# 2-Day Distributed Systems Workshop Agenda

> **Workshop Format**: Each teaching block is 45 minutes (≈ 25 min explanation + 10 min Python analysis + 10 min guided coding)  
> **Breaks**: 10–15 minutes between sessions  
> **Daily Structure**: Day 1: ~3.75 hours teaching (5 sessions), Day 2: ~3 hours teaching (4 sessions) + ~45 minutes of breaks per day

---

## 🛠️ **Workshop Setup** (5 minutes)
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

## 📅 Day 1: Foundations & Basic Patterns

### **Session 1** (45 min) 🎯 **Why Distribute?**
- **Learning Goals:**
  - Resource ceilings and physical limits
  - Little's Law and performance modeling
  - Motivation for distributed patterns
- **🛠️ Hands-on Lab:** Run provided disk-perf test; capture own numbers
- **📊 Performance Analysis (NEW!):**
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
- **💡 Connection:** "This is WHY we need distributed systems - single machines hit performance walls!"
- **Break:** 10 minutes

### **Session 2** (45 min) 🎯 **Why Patterns? & Partial Failure Mindset**
- **Learning Goals:**
  - Understanding the need for distributed patterns
  - Pattern-based thinking in distributed systems
  - Probability of failure at scale and network partitions
  - Process pauses and their impact
- **🛠️ Hands-on Lab:** 
  - Overview of patterns available in the framework
  - Walkthrough of the 'replicate' framework with fault injection
- **📊 Failure Probability Analysis (NEW!):**
  ```bash
  # Calculate realistic failure probabilities
  python failure_probability.py
  
  # Example scenarios to try:
  # Scenario 1: 3 nodes, 2 failures, 0.1 failure rate → ~2.7% chance of losing majority
  # Scenario 2: 5 nodes, 3 failures, 0.05 failure rate → ~0.13% chance of losing majority
  # Scenario 3: Large cluster - 100 nodes, 30 failures, 0.05 failure rate
  ```
  **Key Insights:**
  - Even with 5% individual failure rate, losing quorum is significant risk
  - Larger clusters provide better fault tolerance
  - Patterns help us handle these inevitable failures systematically
- **💡 Connection:** "Patterns solve recurring problems - especially failure handling!"
- **📁 Reference:** `src/main/java/replicate/common/` and `src/test/java/replicate/common/`
- **Break:** 10 minutes

### **Session 3** (45 min) 🎯 **Write-Ahead Log Pattern**
- **Learning Goals:**
  - Append-only discipline for durability
  - Recovery mechanisms and replay
  - WAL as foundation for other patterns
- **🛠️ Hands-on Lab:** Execute and walkthrough `DurableKVStoreTest` for persistent key-value store
- **💡 Connection:** "WAL ensures we can recover from the failures we just discussed!"
- **📁 Reference:** `src/test/java/replicate/wal/DurableKVStoreTest.java`
- **Break:** 15 minutes

### **Session 4** (45 min) 🎯 **Core Communication Patterns**
- **Learning Goals:**
  - Request-waiting list pattern for async operations
  - Singular update queue for thread safety
  - Network messaging foundations
  - Building blocks for distributed protocols
- **🛠️ Hands-on Lab:** 
  - Code walkthrough: `RequestWaitingList` and `SingularUpdateQueue` implementations
  - Understand how async requests are tracked and managed
  - See how single-threaded execution prevents race conditions
- **📁 Reference:** 
  - `src/main/java/replicate/common/RequestWaitingList.java`
  - `src/main/java/replicate/common/SingularUpdateQueue.java`
  - `src/main/java/replicate/net/` - Network communication layer
- **💡 Connection:** "These patterns are the foundation for quorum-based systems and consensus algorithms!"
- **Break:** 10 minutes

### **Session 5** (45 min) 🎯 **Replication & Majority Quorum**
- **Learning Goals:**
  - Write vs read quorums trade-offs
  - Quorum intersection properties
  - Universal Scalability Law curve analysis
- **🛠️ Hands-on Lab:** Modify `QuorumKVStoreTest`: pass for 5-node/3-node clusters
  - **Prerequisite:** Understanding of `RequestWaitingList` from Session 4 (used in quorum coordination)
- **📊 Scalability Analysis (NEW!):**
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
- **💡 Connection:** "This shows the trade-offs in quorum-based replication!"
- **📁 Reference:** `src/test/java/replicate/quorum/QuorumKVStoreTest.java`
- **End of Day 1**

### 🍽️ **Lunch Break / Self-Paced Time**
**Offline Activities:**
- Review morning labs and concepts
- Push completed work to GitHub
- Optional: Explore additional resources
- **NEW:** Experiment with different parameters in Python scripts

---

## 📅 Day 2: Consensus Algorithms & Advanced Patterns

### **Session 6** (45 min) 🎯 **Why Simple Replication Fails**
- **Learning Goals:**
  - Two-phase commit pitfalls
  - Recovery ambiguity problems
  - The need for consensus algorithms
- **🛠️ Hands-on Lab:** Step through `DeferredCommitmentTest` and `RecoverableDeferredCommitmentTest`; explain why they hang
- **📊 Realistic System Behavior Analysis (NEW!):**
  ```bash
  # Show how systems degrade under stress (unlike theoretical models)
  python realistic_system_performance.py
  ```
  **Key Visualizations:**
  1. **Realistic Performance Under Load** - Shows system degradation beyond theoretical limits
  2. **Ideal vs Realistic Comparison** - Why real systems perform worse than theory
  
  **Key Insights:**
  - Systems don't just hit limits - they degrade badly under stress
  - Performance collapse happens before theoretical limits
  - Real systems exhibit much worse behavior than M/M/1 queue models
- **💡 Connection:** "This is exactly why 2PC fails under load - systems don't gracefully degrade!"
- **📁 Reference:** `src/test/java/replicate/twophaseexecution/DeferredCommitmentTest.java`
- **Break:** 10 minutes

### **Session 7** (45 min) 🎯 **Single-Value Paxos**
- **Learning Goals:**
  - Prepare/Accept phases explained
  - Recovery with generation numbers
  - Safety and liveness properties
- **🛠️ Hands-on Lab:** Work with generation voting mechanism using existing Paxos tests
- **📁 Reference:** `src/test/java/replicate/paxos/` and `src/test/java/replicate/generationvoting/`
- **Break:** 10 minutes

### **Session 8** (45 min) 🎯 **From Paxos to Multi-Paxos**
- **Learning Goals:**
  - Replicated log concept and implementation
  - High-water mark for safe execution
  - Heartbeats and failure detection
- **🛠️ Hands-on Lab:** Extend log to multi-slot using Multi-Paxos and Paxos Log implementations
- **📁 Reference:** `src/test/java/replicate/multipaxos/` and `src/test/java/replicate/paxoslog/`
- **Break:** 15 minutes

### **Session 9** (45 min) 🎯 **RAFT vs Multi-Paxos in Practice**
- **Learning Goals:**
  - Implementation optimizations comparison
  - Idempotent receiver pattern
  - Production considerations and future directions
- **🛠️ Hands-on Lab:** Compare RAFT & Multi-Paxos implementations; annotate pros/cons
- **📊 Consensus Algorithm Performance Comparison (NEW!):**
  ```bash
  # Re-run the scalability analysis focusing on consensus algorithms
  python universal_scalability_law_improved.py
  # Focus on the "Consensus Algorithm Performance Comparison" graphs
  ```
  **Discussion Points:**
  - **RAFT vs Multi-Paxos**: Which scales better and why?
  - **Optimal cluster sizes**: 3, 5, 7, or more nodes?
  - **Byzantine Fault Tolerance**: Performance cost analysis
  - **Production trade-offs**: Performance vs complexity vs reliability
  
  **Key Insights:**
  - RAFT typically has lower coordination overhead than basic Paxos
  - Multi-Paxos (optimized) can outperform RAFT in some scenarios
  - Byzantine protocols have significant performance penalties
  - Optimal cluster size is algorithm-dependent
- **💡 Connection:** "Now you have quantitative data to choose algorithms, not just theoretical knowledge!"
- **📁 Reference:** `src/main/java/replicate/raft/` and `src/main/java/replicate/multipaxos/`
- **End of Day 2**

---

## 📊 Workshop Summary

### 🎯 **Enhanced Learning Outcomes**
- **9 teaching blocks** with optimized timing (5 sessions Day 1, 4 sessions Day 2)
- **Pattern-driven learning** progression from motivation to implementation
- **Combined foundational concepts** for efficient learning progression
- **Core patterns foundation** before advanced algorithms
- **Quantitative analysis** integrated with hands-on labs  
- **Visual performance data** reinforcing theoretical concepts
- **Data-driven decision making** for distributed system design

### 🛠️ **Technical Skills Gained**
- Understanding distributed systems fundamentals
- **NEW:** Performance modeling and capacity planning
- **NEW:** Failure probability analysis for reliability planning
- **NEW:** Scalability analysis using Universal Scalability Law
- Implementing Write-Ahead Log pattern
- Working with quorum-based replication
- Exploring consensus algorithms (Paxos, RAFT)
- Hands-on experience with fault tolerance patterns

### 📊 **Performance Analysis Tools**
- **Queuing Theory Analysis**: System performance limits and Little's Law
- **Failure Probability Calculator**: Risk assessment for cluster sizing
- **Universal Scalability Law**: Performance scaling analysis
- **Realistic Performance Modeling**: System degradation under stress
- **Consensus Algorithm Comparison**: Quantitative algorithm selection

### 🗂️ **Available Implementations**
- **Consensus Algorithms:** Paxos, Multi-Paxos, RAFT, ViewStamped Replication
- **Replication Patterns:** Chain Replication, Quorum-based KV Store
- **Foundational Patterns:** WAL, Two-Phase Commit, Heartbeat Detection
- **Network Layer:** Socket-based messaging, Request-waiting lists
- **NEW:** **Performance Analysis Scripts:** Python-based modeling tools

### 📁 **Key Files Reference**
- **Core Framework:** `src/main/java/replicate/common/`
- **WAL Implementation:** `src/main/java/replicate/wal/DurableKVStore.java`
- **Quorum KV Store:** `src/main/java/replicate/quorum/QuorumKVStore.java`
- **Chain Replication:** `src/main/java/replicate/chain/ChainReplication.java`
- **Paxos Implementation:** `src/main/java/replicate/paxos/`
- **RAFT Implementation:** `src/main/java/replicate/raft/`
- **Tests Directory:** `src/test/java/replicate/`
- **NEW:** **Performance Scripts:** `src/main/python/`
  - `queuing_theory.py` - Little's Law and performance analysis
  - `failure_probability.py` - Cluster reliability analysis  
  - `universal_scalability_law_improved.py` - Scaling and algorithm comparison
  - `realistic_system_performance.py` - Real-world performance modeling

### 📚 **Resources & Next Steps**
- All code examples and labs available on GitHub
- **NEW:** Take-home performance analysis scripts for production use
- Additional reading materials provided
- Follow-up Q&A session for complex topics
- **NEW:** Quantitative foundation for architecture decisions

### 💡 **Workshop Enhancement Benefits**
- **Visual Learning**: Graphs and charts reinforce abstract concepts
- **Quantitative Understanding**: Real numbers behind theoretical concepts  
- **Practical Tools**: Scripts participants can use in production
- **Data-Driven Decisions**: Choose algorithms based on performance data
- **Business Impact**: Connect technical decisions to business outcomes
