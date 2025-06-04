package replicate.chain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.chain.messages.*;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.net.requestwaitinglist.RequestCallback;
import replicate.net.requestwaitinglist.RequestWaitingList;
import replicate.mpaxoswithheartbeats.HeartbeatRequest;
import replicate.common.MonotonicId;
import replicate.quorum.messages.GetValueRequest;
import replicate.quorum.messages.GetValueResponse;
import replicate.quorum.StoredValue;
import replicate.wal.SetValueCommand;
import replicate.twophaseexecution.messages.ExecuteCommandResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.io.Serializable;
import replicate.common.MessagePayload;

class Configuration {
    List<InetAddressAndPort> sortedReplicaAddresses;

    public Configuration(List<InetAddressAndPort> replicas) {
        List<InetAddressAndPort> lst = new ArrayList<>(replicas);
        Collections.sort(lst);
        sortedReplicaAddresses = Collections.unmodifiableList(lst);
    }

    public int replicaIndex(InetAddressAndPort address) {
        return sortedReplicaAddresses.indexOf(address);
    }

    public List<InetAddressAndPort> getSortedReplicas() {
        return sortedReplicaAddresses;
    }
}

public class ChainReplication extends Replica {
    private static Logger logger = LogManager.getLogger(ChainReplication.class);

    // Chain configuration
    private NodeRole role;
    private InetAddressAndPort successor;
    private InetAddressAndPort predecessor;
    private final Configuration configuration;

    // State
    private final Map<String, String> durableStore = new ConcurrentHashMap<>();
    private final Map<String, String> store = new ConcurrentHashMap<>();
    private final Map<UUID, PendingWrite> pendingWrites = new ConcurrentHashMap<>();
    private static final String PENDING_WRITES_PREFIX = "__pending_write__";
    private static final String PENDING_WRITES_META_PREFIX = "__pending_write_meta__";
    private int currentVersion = 0;

    // Track pending writes and their status
    private final Map<UUID, WriteStatus> writeStatusMap = new ConcurrentHashMap<>();

    // Track keys that have pending writes
    private final Set<String> keysWithPendingWrites = new ConcurrentHashMap<String, Boolean>().keySet(true);

    // Heartbeat tracking
    private final Map<InetAddressAndPort, Long> lastHeartbeatTimes = new ConcurrentHashMap<>();

    // Version management
    private long lamportClock = 0;
    private final VersionGenerator versionGenerator;

    // Operation types supported by the chain
    public enum OperationType {
        SET,           // Set a value
        INCREMENT,     // Increment a numeric value
        DECREMENT,     // Decrement a numeric value
        APPEND,        // Append to a list
        REMOVE,        // Remove from a list
        GET,          // Get a single value
        RANGE,        // Get a range of values
        AGGREGATE     // Compute an aggregate (sum, count, etc)
    }

    static class ChainOperation extends MessagePayload implements Serializable {
        private static final long serialVersionUID = 1L;
        final OperationType type;
        final String key;
        final Object value;
        final Map<String, Object> parameters;
        final UUID requestId;

        ChainOperation(OperationType type, String key, Object value, Map<String, Object> parameters, UUID requestId) {
            super(MessageId.ChainOperation);
            this.type = type;
            this.key = key;
            this.value = value;
            this.parameters = parameters != null ? parameters : new HashMap<>();
            this.requestId = requestId;
        }

        boolean isUpdate() {
            return type == OperationType.SET || 
                   type == OperationType.INCREMENT || 
                   type == OperationType.DECREMENT ||
                   type == OperationType.APPEND ||
                   type == OperationType.REMOVE;
        }

        boolean isQuery() {
            return type == OperationType.GET ||
                   type == OperationType.RANGE ||
                   type == OperationType.AGGREGATE;
        }
    }

    static class WriteStatus {
        final ChainOperation request;
        boolean isAcknowledged;
        
        WriteStatus(ChainOperation request) {
            this.request = request;
            this.isAcknowledged = false;
        }
    }

    static class PendingWrite {
        final String key;
        final VersionedValue value;
        final UUID requestId;
        boolean isAcknowledged;
        boolean isCommitted;

        PendingWrite(String key, VersionedValue value, UUID requestId) {
            this.key = key;
            this.value = value;
            this.requestId = requestId;
            this.isAcknowledged = false;
            this.isCommitted = false;
        }
    }

    static class ChainWriteAck extends MessagePayload {
        final UUID requestId;
        final boolean isCommit;  // Indicates if this is commit phase

        ChainWriteAck(UUID requestId, boolean isCommit) {
            super(MessageId.ChainWriteAck);
            this.requestId = requestId;
            this.isCommit = isCommit;
        }
    }

    static class ChainWriteRequest extends MessagePayload implements Serializable {
        private static final long serialVersionUID = 1L;
        final ChainOperation operation;
        
        ChainWriteRequest(ChainOperation operation) {
            super(MessageId.ChainWrite);
            this.operation = operation;
        }
    }

    static class Version implements Comparable<Version>, Serializable {
        private static final long serialVersionUID = 1L;
        final long timestamp;
        final int nodeId;
        final int sequence;
        
        Version(long timestamp, int nodeId, int sequence) {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
            this.sequence = sequence;
        }
        
        @Override
        public int compareTo(Version other) {
            int timeCompare = Long.compare(this.timestamp, other.timestamp);
            if (timeCompare != 0) return timeCompare;
            
            int nodeCompare = Integer.compare(this.nodeId, other.nodeId);
            if (nodeCompare != 0) return nodeCompare;
            
            return Integer.compare(this.sequence, other.sequence);
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Version version = (Version) o;
            return timestamp == version.timestamp &&
                   nodeId == version.nodeId &&
                   sequence == version.sequence;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(timestamp, nodeId, sequence);
        }
    }
    
    static class VersionGenerator {
        private final int nodeId;
        private final SystemClock clock;
        private long lastTimestamp;
        private int sequence;
        
        VersionGenerator(int nodeId, SystemClock clock) {
            this.nodeId = nodeId;
            this.clock = clock;
            this.lastTimestamp = clock.nanoTime();
            this.sequence = 0;
        }
        
        synchronized Version nextVersion() {
            long now = clock.nanoTime();
            
            if (now <= lastTimestamp) {
                sequence++;
            } else {
                sequence = 0;
                lastTimestamp = now;
            }
            
            return new Version(lastTimestamp, nodeId, sequence);
        }
    }
    
    static class VersionedValue implements Serializable {
        private static final long serialVersionUID = 1L;
        final Object value;
        final Version version;
        final InetAddressAndPort writer;
        volatile boolean dirty;
        VersionedValue next;
        
        VersionedValue(Object value, Version version, InetAddressAndPort writer) {
            this.value = value;
            this.version = version;
            this.writer = writer;
            this.dirty = true;
            this.next = null;
        }
    }

    static class KeyVersionChain {
        private VersionedValue head;
        private VersionedValue tail;
        private final Map<Version, VersionedValue> versionIndex = new HashMap<>();

        void addVersion(VersionedValue value) {
            if (head == null) {
                head = tail = value;
            } else {
                // Insert in version order
                if (value.version.compareTo(head.version) < 0) {
                    value.next = head;
                    head = value;
                } else {
                    VersionedValue current = head;
                    while (current.next != null && current.next.version.compareTo(value.version) < 0) {
                        current = current.next;
                    }
                    value.next = current.next;
                    current.next = value;
                    if (value.next == null) {
                        tail = value;
                    }
                }
            }
            versionIndex.put(value.version, value);
        }

        VersionedValue getLatestVersion() {
            return tail;
        }

        VersionedValue getVersion(Version version) {
            return versionIndex.get(version);
        }
    }

    // Add version chains to track writes per key
    private final Map<String, KeyVersionChain> versionChains = new ConcurrentHashMap<>();

    static class HeadRecoveryState {
        private long maxSeenTimestamp;
        private final Map<String, List<PendingWrite>> pendingWritesByKey;
        private final Set<UUID> acknowledgedWrites;

        HeadRecoveryState() {
            this.maxSeenTimestamp = 0;
            this.pendingWritesByKey = new HashMap<>();
            this.acknowledgedWrites = new HashSet<>();
        }

        void updateMaxTimestamp(long timestamp) {
            maxSeenTimestamp = Math.max(maxSeenTimestamp, timestamp);
        }

        void addPendingWrite(PendingWrite write) {
            pendingWritesByKey.computeIfAbsent(write.key, k -> new ArrayList<>()).add(write);
            if (write.isAcknowledged) {
                acknowledgedWrites.add(write.requestId);
            }
        }

        List<PendingWrite> getPendingWritesForKey(String key) {
            return pendingWritesByKey.getOrDefault(key, Collections.emptyList());
        }
    }

    // State tracking variables
    private long currentTerm = 0;
    private InetAddressAndPort currentLeader;
    private long lastCommittedIndex = 0;
    private long lastAppliedIndex = 0;

    public ChainReplication(String name, Config config, SystemClock clock,
                          InetAddressAndPort clientConnectionAddress,
                          InetAddressAndPort peerConnectionAddress,
                          List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
        this.configuration = new Configuration(peerAddresses);
        // Initialize version generator with node ID from peer address position
        int nodeId = configuration.replicaIndex(peerConnectionAddress);
        this.versionGenerator = new VersionGenerator(nodeId, clock);
        setupChainConfiguration();
    }

    @Override
    protected void registerHandlers() {
        handlesMessage(MessageId.ChainOperation, this::handleClientOperation, ChainOperation.class);
        handlesMessage(MessageId.ChainWrite, this::handleChainWrite, ChainWriteRequest.class);
        handlesMessage(MessageId.ChainWriteAck, this::handleChainWriteAck, ChainWriteAck.class);
        handlesMessage(MessageId.Recovery, this::handleRecoveryRequest, RecoveryRequest.class);
        handlesMessage(MessageId.RecoveryResponse, this::handleRecoveryResponse, RecoveryResponse.class);
        handlesMessage(MessageId.ExcuteCommandRequest, this::handleExecuteCommand, replicate.twophaseexecution.messages.ExecuteCommandRequest.class);
    }

    private CompletableFuture<ExecuteCommandResponse> handleExecuteCommand(Message<replicate.twophaseexecution.messages.ExecuteCommandRequest> message) {
        // Convert ExecuteCommandRequest to ChainOperation
        replicate.twophaseexecution.messages.ExecuteCommandRequest request = message.messagePayload();
        
        // Deserialize the command from the request
        SetValueCommand command = JsonSerDes.deserialize(request.command, SetValueCommand.class);
        
        // Create ChainOperation
        ChainOperation operation = new ChainOperation(
            OperationType.SET, 
            command.getKey(), 
            command.getValue(), 
            null, 
            UUID.randomUUID()
        );
        
        // Create new message with ChainOperation using a new header
        Message.Header newHeader = new Message.Header(
            message.getFromAddress(),
            message.getCorrelationId(),
            MessageId.ChainOperation
        );
        Message<ChainOperation> chainMessage = new Message<>(
            operation,
            newHeader
        );
        
        return handleClientOperation(chainMessage);
    }

    private CompletableFuture<ExecuteCommandResponse> handleClientOperation(Message<ChainOperation> message) {
        ChainOperation operation = message.messagePayload();
        // For update operations, must go through head
        if (operation.isUpdate() && role != NodeRole.HEAD) {
            CompletableFuture<ExecuteCommandResponse> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Update operations must be sent to the head node"));
            return future;
        }

        // For query operations, must go to tail
        if (operation.isQuery() && role != NodeRole.TAIL) {
            CompletableFuture<ExecuteCommandResponse> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Query operations must be sent to the tail node"));
            return future;
        }

        if (operation.isUpdate()) {
            // Check if there's already a pending write for this key
            if (!keysWithPendingWrites.add(operation.key)) {
                CompletableFuture<ExecuteCommandResponse> future = new CompletableFuture<>();
                future.completeExceptionally(new IllegalStateException("Another write is pending for key: " + operation.key));
                return future;
            }

            CompletableFuture<ExecuteCommandResponse> future = new CompletableFuture<>();
            RequestCallback<ExecuteCommandResponse> callback = new RequestCallback<ExecuteCommandResponse>() {
                @Override
                public void onResponse(ExecuteCommandResponse response, InetAddressAndPort from) {
                    // Remove key from pending set when write is complete
                    keysWithPendingWrites.remove(operation.key);
                    future.complete(response);
                }

                @Override
                public void onError(Exception e) {
                    // Remove key from pending set on error
                    keysWithPendingWrites.remove(operation.key);
                    future.completeExceptionally(e);
                }
            };
            requestWaitingList.add(operation.requestId.toString(), callback);
            writeStatusMap.put(operation.requestId, new WriteStatus(operation));
            sendOneway(successor, new ChainWriteRequest(operation), correlationId());
            return future;
        } else {
            return handleQueryOperation(operation);
        }
    }

    private CompletableFuture<ExecuteCommandResponse> handleQueryOperation(ChainOperation operation) {
        Object result = null;
        switch (operation.type) {
            case GET:
                // Get the latest committed version
                KeyVersionChain versionChain = versionChains.get(operation.key);
                if (versionChain != null) {
                    VersionedValue latest = versionChain.getLatestVersion();
                    if (latest != null && !latest.dirty) {
                        result = latest.value;
                    }
                }
                if (result == null) {
                    result = store.get(operation.key);
                }
                break;
            case RANGE:
                String startKey = (String) operation.parameters.get("startKey");
                String endKey = (String) operation.parameters.get("endKey");
                result = store.entrySet().stream()
                    .filter(e -> e.getKey().compareTo(startKey) >= 0 && e.getKey().compareTo(endKey) <= 0)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                break;
            case AGGREGATE:
                String aggregateType = (String) operation.parameters.get("type");
                if ("COUNT".equals(aggregateType)) {
                    result = store.size();
                } else if ("SUM".equals(aggregateType)) {
                    result = store.values().stream()
                        .mapToInt(Integer::parseInt)
                        .sum();
                }
                break;
        }
        return CompletableFuture.completedFuture(
            new ExecuteCommandResponse(Optional.of(result != null ? result.toString() : ""), true)
        );
    }

    private void applyUpdate(ChainOperation operation) {
        switch (operation.type) {
            case SET:
                store.put(operation.key, operation.value.toString());
                break;
            case INCREMENT:
                int currentValue = Integer.parseInt(store.getOrDefault(operation.key, "0"));
                store.put(operation.key, String.valueOf(currentValue + (Integer)operation.value));
                break;
            case DECREMENT:
                currentValue = Integer.parseInt(store.getOrDefault(operation.key, "0"));
                store.put(operation.key, String.valueOf(currentValue - (Integer)operation.value));
                break;
            case APPEND:
                String currentList = store.getOrDefault(operation.key, "[]");
                // Assume JSON array format for lists
                List<Object> list = new ArrayList<>();  // Parse currentList as JSON
                list.add(operation.value);
                store.put(operation.key, list.toString());  // Store as JSON
                break;
            case REMOVE:
                currentList = store.getOrDefault(operation.key, "[]");
                // Remove value from JSON array
                // Implementation depends on JSON library
                break;
        }
    }

    private void storePendingWrite(String key, VersionedValue versionedValue) {
        // Store both the value and its metadata
        String valueKey = PENDING_WRITES_PREFIX + key + "_" + versionedValue.version.timestamp;
        String metaKey = PENDING_WRITES_META_PREFIX + key + "_" + versionedValue.version.timestamp;
        
        durableStore.put(valueKey, versionedValue.value.toString());
        String metadata = String.format("%d,%d,%d,%s,%b",
            versionedValue.version.timestamp,
            versionedValue.version.nodeId,
            versionedValue.version.sequence,
            versionedValue.writer.toString(),
            versionedValue.dirty);
        durableStore.put(metaKey, metadata);
    }

    private void loadPendingWritesFromDurable() {
        for (Map.Entry<String, String> entry : durableStore.entrySet()) {
            if (entry.getKey().startsWith(PENDING_WRITES_META_PREFIX)) {
                String requestIdStr = entry.getKey().substring(PENDING_WRITES_META_PREFIX.length());
                UUID requestId = UUID.fromString(requestIdStr);
                
                String[] parts = entry.getValue().split(",");
                String key = parts[0];
                boolean isAcknowledged = Boolean.parseBoolean(parts[2]);
                boolean isCommitted = Boolean.parseBoolean(parts[3]);
                long version = Long.parseLong(parts[4]);
                
                String value = durableStore.get(PENDING_WRITES_PREFIX + requestId);
                if (value != null) {
                    PendingWrite write = new PendingWrite(key, new VersionedValue(value, null, null), requestId);
                    write.isAcknowledged = isAcknowledged;
                    write.isCommitted = isCommitted;
                    pendingWrites.put(requestId, write);
                    
                    // Update Lamport clock if needed
                    if (version > lamportClock) {
                        lamportClock = version;
                    }
                    
                    // Only make visible if committed
                    if (isCommitted) {
                        store.put(key, value);
                    }
                }
            }
        }
    }

    private void handleChainWrite(Message<ChainWriteRequest> message) {
        ChainOperation operation = message.messagePayload().operation;
        // Generate new version
        Version version = versionGenerator.nextVersion();
        
        // Create new versioned value
        VersionedValue newVersion = new VersionedValue(
            operation.value,
            version,
            message.getFromAddress()
        );
        
        // Add to version chain for the key
        versionChains.computeIfAbsent(operation.key, k -> new KeyVersionChain())
                    .addVersion(newVersion);
        
        // Create and store the pending write
        PendingWrite pendingWrite = new PendingWrite(operation.key, newVersion, operation.requestId);
        pendingWrites.put(operation.requestId, pendingWrite);
        
        // Store the write
        storePendingWrite(operation.key, newVersion);
        
        if (role == NodeRole.TAIL) {
            sendOneway(predecessor, new ChainWriteAck(operation.requestId, false), correlationId());
        } else {
            // Forward the operation with the new version
            sendOneway(successor, new ChainWriteRequest(operation), correlationId());
        }
    }

    private void handleChainWriteAck(Message<ChainWriteAck> message) {
        ChainWriteAck ack = message.messagePayload();
        PendingWrite pending = pendingWrites.get(ack.requestId);
        
        if (pending == null) {
            logger.warn("Received ack for unknown write: {}", ack.requestId);
            return;
        }

        if (!ack.isCommit) {
            // Phase 1: Prepare acknowledgment
            // Ensure write is durably stored
            if (!isDurablelyStored(pending)) {
                storePendingWrite(pending.key, pending.value);
            }
            
            if (role == NodeRole.HEAD) {
                // Head received prepare ack from all nodes
                // Start commit phase only after durable storage
                pending.isAcknowledged = true;
                sendOneway(successor, new ChainWriteAck(ack.requestId, true), correlationId());
            } else {
                // Forward prepare ack up the chain
                sendOneway(predecessor, ack, correlationId());
            }
        } else {
            // Phase 2: Commit
            pending.isCommitted = true;
            makeWriteVisible(ack.requestId);
            
            if (role == NodeRole.HEAD) {
                // Head completes client request after commit phase
                WriteStatus status = writeStatusMap.get(ack.requestId);
                if (status != null) {
                    status.isAcknowledged = true;
                }
                requestWaitingList.handleResponse(ack.requestId.toString(), 
                    new ExecuteCommandResponse(Optional.of("OK"), true),
                    message.getFromAddress());
            } else if (role != NodeRole.TAIL) {
                // Forward commit message down the chain
                sendOneway(successor, ack, correlationId());
            }
        }
    }

    private boolean isDurablelyStored(PendingWrite write) {
        return durableStore.containsKey(PENDING_WRITES_PREFIX + write.requestId) &&
               durableStore.containsKey(PENDING_WRITES_META_PREFIX + write.requestId);
    }

    private void makeWriteVisible(UUID requestId) {
        PendingWrite pending = pendingWrites.get(requestId);
        if (pending != null && pending.isCommitted) {
            // Get the version chain for this key
            KeyVersionChain versionChain = versionChains.get(pending.key);
            if (versionChain != null) {
                // Only make visible if this is the latest version
                VersionedValue latest = versionChain.getLatestVersion();
                if (latest == pending.value) {
                    store.put(pending.key, pending.value.value.toString());
                }
            }
            storePendingWrite(pending.key, pending.value);  // Ensure durability
        }
    }

    private void setupChainConfiguration() {
        List<InetAddressAndPort> peers = configuration.getSortedReplicas();
        int myIndex = peers.indexOf(getPeerConnectionAddress());

        if (myIndex == 0) {
            role = NodeRole.HEAD;
            predecessor = null;
            successor = peers.get(myIndex + 1);
        } else if (myIndex == peers.size() - 1) {
            role = NodeRole.TAIL;
            predecessor = peers.get(myIndex - 1);
            successor = null;
        } else {
            role = NodeRole.MIDDLE;
            predecessor = peers.get(myIndex - 1);
            successor = peers.get(myIndex + 1);
        }
        
        logger.info("{} configured as {} with predecessor={} successor={}", 
            getName(), role, predecessor, successor);
    }

    public String getValue(String key) {
        if (role != NodeRole.TAIL) {
            throw new IllegalStateException("Read requests must be served by the tail node");
        }
        return store.get(key);
    }

    public NodeRole getRole() {
        return role;
    }

    public InetAddressAndPort getSuccessor() {
        return successor;
    }

    public InetAddressAndPort getPredecessor() {
        return predecessor;
    }

    private int correlationId() {
        return ThreadLocalRandom.current().nextInt();
    }

    public void handleNodeFailure(InetAddressAndPort failedNode) {
        List<InetAddressAndPort> peers = configuration.getSortedReplicas();
        int failedIndex = peers.indexOf(failedNode);
        int myIndex = peers.indexOf(getPeerConnectionAddress());
        
        if (failedNode.equals(successor)) {
            // Successor failed
            if (failedIndex < peers.size() - 1) {
                // Connect to next available node
                successor = peers.get(failedIndex + 1);
                // Request recovery to ensure we have all updates
                sendOneway(successor, new RecoveryRequest(role), correlationId());
            } else {
                // I become the new tail
                role = NodeRole.TAIL;
                successor = null;
                // Process all prepared but uncommitted writes
                for (PendingWrite pending : pendingWrites.values()) {
                    if (pending.isAcknowledged && !pending.isCommitted) {
                        // These writes were prepared but not committed
                        // Need to restart prepare phase
                        sendOneway(predecessor, new ChainWriteAck(pending.requestId, false), correlationId());
                    }
                }
            }
            logger.info("{} handling successor failure. New role={}, successor={}", getName(), role, successor);
        } else if (failedNode.equals(predecessor)) {
            if (failedIndex > 0) {
                // Connect to previous available node
                predecessor = peers.get(failedIndex - 1);
                sendOneway(predecessor, new RecoveryRequest(role), correlationId());
            } else {
                // I become the new head
                onBecomeHead();
            }
            logger.info("{} handling predecessor failure. New role={}, predecessor={}", getName(), role, predecessor);
        }
        
        startRecovery();
    }

    private void startRecovery() {
        // Load any pending writes from durable storage
        loadPendingWritesFromDurable();
        
        // Request recovery data from both neighbors if available
        if (successor != null) {
            sendOneway(successor, new RecoveryRequest(role), correlationId());
        }
        if (predecessor != null) {
            sendOneway(predecessor, new RecoveryRequest(role), correlationId());
        }
        
        // If we're the tail, ensure all committed writes are visible
        if (role == NodeRole.TAIL) {
            for (PendingWrite write : pendingWrites.values()) {
                if (write.isCommitted) {
                    makeWriteVisible(write.requestId);
                }
            }
        }
    }

    @Override
    protected void checkLeader() {
        // In chain replication, we need to check both predecessor and successor
        Duration timeSinceLastHeartbeat = elapsedTimeSinceLastHeartbeat();
        if (timeSinceLastHeartbeat.compareTo(heartbeatTimeout) > 0) {
            if (predecessor != null && !hasHeartbeat(predecessor)) {
                logger.info("{} detected predecessor failure: {}", getName(), predecessor);
                handleNodeFailure(predecessor);
            }
            if (successor != null && !hasHeartbeat(successor)) {
                logger.info("{} detected successor failure: {}", getName(), successor);
                handleNodeFailure(successor);
            }
        }
    }

    private boolean hasHeartbeat(InetAddressAndPort node) {
        Long lastHeartbeat = getLastHeartbeatTime(node);
        if (lastHeartbeat == null) {
            return false;
        }
        Duration timeSinceLastHeartbeat = Duration.ofNanos(clock.nanoTime() - lastHeartbeat);
        return timeSinceLastHeartbeat.compareTo(heartbeatTimeout) <= 0;
    }

    @Override
    protected void sendHeartbeats() {
        // Send heartbeats only to immediate neighbors instead of all nodes
        if (predecessor != null) {
            sendOneway(predecessor, new HeartbeatRequest(MonotonicId.empty()), correlationId());
        }
        if (successor != null) {
            sendOneway(successor, new HeartbeatRequest(MonotonicId.empty()), correlationId());
        }
    }

    protected void recordHeartbeat(InetAddressAndPort from) {
        updateLastHeartbeatTime(from, clock.nanoTime());
    }

    // Recovery protocol messages
    static class RecoveryRequest extends MessagePayload {
        final NodeRole requestingRole;
        
        RecoveryRequest(NodeRole role) {
            super(MessageId.Recovery);
            this.requestingRole = role;
        }
    }

    static class RecoveryResponse extends MessagePayload {
        final Map<String, String> data;
        final Map<UUID, WriteStatus> writeStatuses;
        final Map<UUID, PendingWrite> pendingWrites;
        
        RecoveryResponse(Map<String, String> data, Map<UUID, WriteStatus> writeStatuses, Map<UUID, PendingWrite> pendingWrites) {
            super(MessageId.RecoveryResponse);
            this.data = data;
            this.writeStatuses = writeStatuses;
            this.pendingWrites = pendingWrites;
        }
    }

    @Override
    protected void onStart() {
        super.onStart();
        // Load any pending writes from durable storage first
        loadPendingWritesFromDurable();
        
        // Request recovery data from both neighbors if available
        // This ensures we get the most up-to-date state
        if (successor != null) {
            sendOneway(successor, new RecoveryRequest(role), correlationId());
        }
        if (predecessor != null) {
            sendOneway(predecessor, new RecoveryRequest(role), correlationId());
        }
        
        // If we're the tail, we need to make all writes visible and send acks
        if (role == NodeRole.TAIL) {
            for (PendingWrite write : pendingWrites.values()) {
                if (!write.isAcknowledged) {
                    makeWriteVisible(write.requestId);
                    sendOneway(predecessor, new ChainWriteAck(write.requestId, false), correlationId());
                }
            }
        }
    }

    private void handleRecoveryRequest(Message<RecoveryRequest> message) {
        RecoveryRequest request = message.messagePayload();
        
        // Send our current state to the recovering node
        Map<UUID, WriteStatus> statusesToSend = new HashMap<>();
        Map<UUID, PendingWrite> pendingWritesToSend = new HashMap<>();

        // Always send pending writes for proper chain consistency
        pendingWritesToSend.putAll(pendingWrites);

        if (request.requestingRole == NodeRole.HEAD) {
            // If head is recovering, send all write statuses
            statusesToSend.putAll(writeStatusMap);
        }
        
        sendOneway(message.getFromAddress(), 
            new RecoveryResponse(durableStore, statusesToSend, pendingWritesToSend), 
            correlationId());
    }

    private void handleRecoveryResponse(Message<RecoveryResponse> message) {
        RecoveryResponse response = message.messagePayload();
        
        // First, update our durable store with any missing entries
        durableStore.putAll(response.data);
        
        // Process pending writes from the response
        for (Map.Entry<UUID, PendingWrite> entry : response.pendingWrites.entrySet()) {
            UUID requestId = entry.getKey();
            PendingWrite receivedWrite = entry.getValue();
            PendingWrite existingWrite = pendingWrites.get(requestId);
            
            if (existingWrite == null) {
                // New pending write we haven't seen
                pendingWrites.put(requestId, receivedWrite);
                storePendingWrite(receivedWrite.key, receivedWrite.value);
                
                if (receivedWrite.isAcknowledged) {
                    // If it was acknowledged in the sender's state, make it visible
                    store.put(receivedWrite.key, receivedWrite.value.value.toString());
                }
            } else if (!existingWrite.isAcknowledged && receivedWrite.isAcknowledged) {
                // We have the write but didn't know it was acknowledged
                makeWriteVisible(requestId);
                
                if (role != NodeRole.HEAD) {
                    // Propagate the acknowledgment up the chain
                    sendOneway(predecessor, new ChainWriteAck(requestId, false), correlationId());
                }
            }
        }

        // If we're the head, also update write statuses
        if (role == NodeRole.HEAD) {
            for (Map.Entry<UUID, WriteStatus> entry : response.writeStatuses.entrySet()) {
                UUID requestId = entry.getKey();
                WriteStatus receivedStatus = entry.getValue();
                WriteStatus existingStatus = writeStatusMap.get(requestId);
                
                if (existingStatus == null || !existingStatus.isAcknowledged) {
                    writeStatusMap.put(requestId, receivedStatus);
                    if (receivedStatus.isAcknowledged) {
                        // Complete any pending client requests
                        requestWaitingList.handleResponse(requestId.toString(),
                            new ExecuteCommandResponse(Optional.of("OK"), true),
                            message.getFromAddress());
                    }
                }
            }
        }

        // If we're the tail, ensure all acknowledged writes are visible
        if (role == NodeRole.TAIL) {
            for (PendingWrite write : pendingWrites.values()) {
                makeWriteVisible(write.requestId);
                // Send ack back up the chain for any previously unacknowledged writes
                sendOneway(predecessor, new ChainWriteAck(write.requestId, false), correlationId());
            }
        }
    }

    private Long getLastHeartbeatTime(InetAddressAndPort node) {
        return lastHeartbeatTimes.get(node);
    }

    private void updateLastHeartbeatTime(InetAddressAndPort node, long time) {
        lastHeartbeatTimes.put(node, time);
    }

    private long generateVersion(Message<?> message) {
        // Update Lamport clock based on message timestamp
        if (message != null && message.getTimestamp() > lamportClock) {
            lamportClock = message.getTimestamp();
        }
        // Increment and return
        return ++lamportClock;
    }

    private void handleHeadFailure() {
        // First, collect all pending writes and their states
        HeadRecoveryState recoveryState = new HeadRecoveryState();
        
        // Update max timestamp from our version chains
        for (KeyVersionChain chain : versionChains.values()) {
            VersionedValue latest = chain.getLatestVersion();
            if (latest != null) {
                recoveryState.updateMaxTimestamp(latest.version.timestamp);
            }
        }

        // Collect all pending writes
        for (PendingWrite write : pendingWrites.values()) {
            recoveryState.addPendingWrite(write);
            if (write.value != null && write.value.version != null) {
                recoveryState.updateMaxTimestamp(write.value.version.timestamp);
            }
        }

        // Update our Lamport clock to be greater than any seen timestamp
        lamportClock = Math.max(lamportClock, recoveryState.maxSeenTimestamp + 1);

        // For each key, reprocess pending writes in version order
        for (Map.Entry<String, List<PendingWrite>> entry : recoveryState.pendingWritesByKey.entrySet()) {
            String key = entry.getKey();
            List<PendingWrite> keyWrites = entry.getValue();
            
            // Sort by version
            keyWrites.sort((w1, w2) -> w1.value.version.compareTo(w2.value.version));
            
            // Reprocess each write
            for (PendingWrite write : keyWrites) {
                if (!write.isCommitted) {
                    // For uncommitted writes, generate new version and restart
                    Version newVersion = versionGenerator.nextVersion();
                    VersionedValue newValue = new VersionedValue(
                        write.value.value,
                        newVersion,
                        getPeerConnectionAddress()  // New head is now the writer
                    );
                    
                    // Update version chain
                    versionChains.computeIfAbsent(key, k -> new KeyVersionChain())
                               .addVersion(newValue);
                    
                    // Create new pending write with new version
                    PendingWrite newWrite = new PendingWrite(key, newValue, write.requestId);
                    pendingWrites.put(write.requestId, newWrite);
                    
                    // Forward to successor
                    ChainOperation operation = writeStatusMap.get(write.requestId).request;
                    sendOneway(successor, new ChainWriteRequest(operation), correlationId());
                }
            }
        }
    }

    protected void onBecomeHead() {
        role = NodeRole.HEAD;
        predecessor = null;
        handleHeadFailure();
        logger.info("{} became head node", getName());
    }

    // Client operation methods
    public CompletableFuture<WriteResult> handleClientWrite(String key, String value) {
        ChainOperation operation = new ChainOperation(
            OperationType.SET,
            key,
            value,
            null,
            UUID.randomUUID()
        );
        return handleClientOperation(new Message<>(operation, null, null))
            .thenApply(response -> WriteResult.success(key, value, lamportClock));
    }

    public CompletableFuture<String> handleClientRead(String key) {
        ChainOperation operation = new ChainOperation(
            OperationType.GET,
            key,
            null,
            null,
            UUID.randomUUID()
        );
        return handleClientOperation(new Message<>(operation, null, null))
            .thenApply(response -> response.getResponse().orElse(null));
    }

    // State access methods
    public String getRoleAsString() {
        return role.name();
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public InetAddressAndPort getCurrentLeader() {
        return currentLeader;
    }

    public Map<String, String> getKeyValueStore() {
        return new HashMap<>(store);
    }

    public Map<String, String> getPendingOperations() {
        Map<String, String> pending = new HashMap<>();
        pendingWrites.forEach((id, write) -> {
            if (!write.isCommitted) {
                pending.put(write.key, write.value.value.toString());
            }
        });
        return pending;
    }

    public long getLastCommittedIndex() {
        return lastCommittedIndex;
    }

    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public InetAddressAndPort getAddress() {
        return getPeerConnectionAddress();
    }

    public List<InetAddressAndPort> getPeers() {
        return new ArrayList<>(configuration.getSortedReplicas());
    }

    public void reset() {
        store.clear();
        pendingWrites.clear();
        writeStatusMap.clear();
        versionChains.clear();
        keysWithPendingWrites.clear();
        lastCommittedIndex = 0;
        lastAppliedIndex = 0;
        currentTerm = 0;
        lamportClock = 0;
    }

    public void shutdown() {
        // Clean up resources
        store.clear();
        pendingWrites.clear();
        writeStatusMap.clear();
        versionChains.clear();
        keysWithPendingWrites.clear();
        // Additional cleanup if needed
    }
}