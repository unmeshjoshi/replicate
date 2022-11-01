package replicate.multipaxos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.*;
import replicate.multipaxos.messages.FullLogPrepareResponse;
import replicate.net.InetAddressAndPort;
import replicate.net.requestwaitinglist.RequestWaitingList;
import replicate.paxos.messages.CommitResponse;
import replicate.paxos.messages.GetValueResponse;
import replicate.paxos.messages.ProposalResponse;
import replicate.paxoslog.PaxosResult;
import replicate.paxoslog.messages.CommitRequest;
import replicate.paxoslog.messages.PrepareRequest;
import replicate.paxoslog.messages.ProposalRequest;
import replicate.quorum.messages.GetValueRequest;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;
import replicate.twophaseexecution.messages.ExecuteCommandResponse;
import replicate.vsr.CompletionCallback;
import replicate.wal.Command;
import replicate.wal.SetValueCommand;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

enum ServerRole {
    Leader, Follower, LookingForLeader
}

public class MultiPaxos extends Replica {
    private static Logger logger = LogManager.getLogger(MultiPaxos.class);
    private final SetValueCommand NO_OP_COMMAND = new SetValueCommand("", "");
    //Paxos State
    Map<Integer, PaxosState> paxosLog = new HashMap<>();
    Map<String, String> kv = new HashMap<>();
    final int serverId;
    ServerRole role;

    public MultiPaxos(String name, SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        super(name, config, clock, clientAddress, peerConnectionAddress, peers);
        this.serverId = config.getServerId();
        requestWaitingList = new RequestWaitingList(clock);
    }

    @Override
    protected void registerHandlers() {
        //client rpc
        handlesRequestAsync(MessageId.ExcuteCommandRequest, this::handleClientExecuteCommand, ExecuteCommandRequest.class);
        handlesRequestAsync(MessageId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);

        //peer to peer message passing
        handlesMessage(MessageId.Prepare, this::handleFullLogPrepare, PrepareRequest.class);
        handlesMessage(MessageId.Promise, this::handleFullLogPrepareResponse, FullLogPrepareResponse.class);

        handlesMessage(MessageId.ProposeRequest, this::handlePaxosProposal, ProposalRequest.class);
        handlesMessage(MessageId.ProposeResponse, this::handleProposalResponse, ProposalResponse.class);

        handlesMessage(MessageId.Commit, this::handlePaxosCommit, CommitRequest.class);
        handlesMessage(MessageId.CommitResponse, this::handleCommitResponse, CommitResponse.class);
    }

    private void handleCommitResponse(Message<CommitResponse> commitResponseMessage) {
        handleResponse(commitResponseMessage);
    }

    private void handleProposalResponse(Message<ProposalResponse> proposalResponseMessage) {
        handleResponse(proposalResponseMessage);
    }

    private void handleFullLogPrepareResponse(Message<FullLogPrepareResponse> fullLogPrepareResponseMessage) {
        handleResponse(fullLogPrepareResponseMessage);

    }

//Following heartbeat implementation can be used for two purposes
// 1. Auto trigger election by checking if existing leader fails.
// 2. For leader to notify followers about its availability.

    @Override
    public void sendHeartbeats() {
//       super.sendOnewayMessageToOtherReplicas(new HeartbeatRequest());
    }

    @Override
    public void checkLeader() {
//
//        Duration timeSinceLastHeartbeat = elapsedTimeSinceLastHeartbeat();
//
//        if (timeSinceLastHeartbeat.compareTo(heartbeatTimeout) > 0) {
//            logger.info(getName() + " heartbeat timedOut after " + timeSinceLastHeartbeat.toMillis() + "ms");
//            this.role = ServerRole.LookingForLeader;
//            heartbeatChecker.stop();
//            heartBeatScheduler.stop();
//            leaderElection();
//        }
    }

    private CompletableFuture<ExecuteCommandResponse> handleClientExecuteCommand(ExecuteCommandRequest t) {
        if (role != ServerRole.Leader) {
            return CompletableFuture.failedFuture(new RuntimeException("Can not process requests as the node is not the leader"));
        }
        var commitCallback = new CompletionCallback<ExecuteCommandResponse>();
        CompletableFuture<PaxosResult> appendFuture = append(t.command, commitCallback);
        return appendFuture.thenCompose(r -> commitCallback.getFuture());
    }

    private CompletableFuture<GetValueResponse> handleClientGetValueRequest(GetValueRequest request) {
        var commitCallback = new CompletionCallback<ExecuteCommandResponse>();
        CompletableFuture<PaxosResult> appendFuture = append(NO_OP_COMMAND.serialize(), commitCallback);
        return appendFuture.thenCompose(r -> commitCallback.getFuture())
                .thenApplyAsync(r -> {
                    return new GetValueResponse(Optional.ofNullable(kv.get(request.getKey())));
                }, singularUpdateQueueExecutor);
    }

    RequestWaitingList requestWaitingList;

    AtomicInteger maxKnownPaxosRoundId = new AtomicInteger(1);
    AtomicInteger logIndex = new AtomicInteger(0);

    public CompletableFuture<PaxosResult> append(byte[] initialValue, CompletionCallback<ExecuteCommandResponse> callback) {
        CompletableFuture<PaxosResult> appendFuture = doPaxos(initialValue, callback);
        return appendFuture.thenCompose((result) -> {
            if (result.value.stream().allMatch(v -> v != initialValue)) {
                logger.info("Could not append proposed value to " + logIndex + ". Trying next index");
                return append(initialValue, callback);
            }
            return CompletableFuture.completedFuture(result);
        });
    }


    private CompletableFuture<PaxosResult> doPaxos(byte[] value, CompletionCallback<ExecuteCommandResponse> callback) {
        return doPaxos(fullLogBallot, logIndex.getAndIncrement(), value, callback);
    }

    private CompletableFuture<PaxosResult> doPaxos(MonotonicId monotonicId, int index, byte[] initialValue, CompletionCallback<ExecuteCommandResponse> callback) {
        return sendProposeRequest(index, initialValue, monotonicId)
                .thenCompose(proposedValue -> {
                    //Once the index at which the command is committed reaches 'high-watermark', return the result.
                    if (proposedValue == initialValue) {
                        requestWaitingList.add(index, callback);
                    }
                    return sendCommitRequest(index, proposedValue, monotonicId)
                            .thenApply(r -> new PaxosResult(Optional.of(proposedValue), true));
                });
    }


    private CompletableFuture<Boolean> sendCommitRequest(int index, byte[] value, MonotonicId monotonicId) {
        AsyncQuorumCallback<CommitResponse> commitCallback = new AsyncQuorumCallback<CommitResponse>(getNoOfReplicas(), c -> c.success);
        sendMessageToReplicas(commitCallback, MessageId.Commit, new CommitRequest(index, value, monotonicId));
        return commitCallback.getQuorumFuture().thenApply(result -> true);
    }


    private CompletableFuture<byte[]> sendProposeRequest(int index, byte[] proposedValue, MonotonicId monotonicId) {
        var proposalCallback = new AsyncQuorumCallback<ProposalResponse>(getNoOfReplicas(), p -> p.success);
        logger.debug(getName() + " proposing " + proposedValue + " for index " + index);
        sendMessageToReplicas(proposalCallback, MessageId.ProposeRequest, new ProposalRequest(monotonicId, index, proposedValue));
        return proposalCallback.getQuorumFuture().thenApply(r -> proposedValue);
    }

    public void leaderElection() {
        logger.info(getName() + " triggering election");
        heartbeatChecker.stop();
        //if future completes successfully, phase1 is complete and this node can be the leader.
        runElection().whenCompleteAsync((result, throwable) -> {
            if (throwable == null) {
                logger.info(getName() + " is leader for " + fullLogBallot);
                this.isLeader = true;
                this.role = ServerRole.Leader;
                heartbeatChecker.stop();
                heartBeatScheduler.start();
            }
        }, singularUpdateQueueExecutor);
    }

    public CompletableFuture<Void> runElection() {
        logger.info(getName() + " triggering election.");
        this.fullLogBallot = new MonotonicId(maxKnownPaxosRoundId.incrementAndGet(), serverId);
        return sendFullLogPrepare(fullLogBallot).thenCompose(prepareResponse -> {
            List<FullLogPrepareResponse> promises = prepareResponse.values().stream().toList();
            for (FullLogPrepareResponse promise : promises) {
                mergeLog(promise);
            }
            return sendProposalRequestsForUnCommittedEntries();
        });
    }

    boolean isLeader = false;

    private CompletableFuture<Void> sendProposalRequestsForUnCommittedEntries() {
        List<CompletableFuture> commitFutures = new ArrayList<>();
        Map<Integer, PaxosState> uncommitedValues = getUncommitedValues();
        for (Integer index : uncommitedValues.keySet()) {
            PaxosState logEntry = uncommitedValues.get(index);
            byte[] proposedValue = logEntry.acceptedValue().get();
            var completeFuture = sendProposeRequest(index, proposedValue, fullLogBallot)
                    .thenCompose(value -> {
                        return sendCommitRequest(index, proposedValue, fullLogBallot);
                    });
            commitFutures.add(completeFuture);
        }
        return CompletableFuture.allOf(commitFutures.toArray(new CompletableFuture[0]));
    }

    private void mergeLog(FullLogPrepareResponse promise) {
        var indexes = promise.uncommittedValues.keySet();
        for (Integer index : indexes) {
            PaxosState peerEntry = promise.uncommittedValues.get(index);
            PaxosState selfEntry = paxosLog.get(index);
            if (selfEntry == null || isAfter(peerEntry.acceptedBallot(), selfEntry.acceptedBallot())) {
                paxosLog.put(index, peerEntry);
            }
        }
    }

    private boolean isAfter(Optional<MonotonicId> m1, Optional<MonotonicId> m2) {
        if (m1.isPresent() && m2.isPresent()) {
            return m1.get().isAfter(m2.get());
        }
        if (m1.isPresent()) {
            return true;
        }

        if (m2.isPresent()) {
            return false;
        }

        return false;
    }

    private CompletableFuture<Map<InetAddressAndPort, FullLogPrepareResponse>> sendFullLogPrepare(MonotonicId fullLogPromisedGeneration) {
        var prepareCallback = new AsyncQuorumCallback<FullLogPrepareResponse>(getNoOfReplicas(), r -> r.promised);
        logger.info(getName() + " sending prepare request for " + fullLogPromisedGeneration);
        sendMessageToReplicas(prepareCallback, MessageId.Prepare, new PrepareRequest(-1, fullLogPromisedGeneration));
        return prepareCallback.getQuorumFuture();
    }

    private void handlePaxosCommit(Message<CommitRequest> message) {
        var request = message.messagePayload();
        var paxosState = getOrCreatePaxosState(request.index);
        //Accept commit, because commit is invoked only after successful prepare and propose.
        PaxosState committedPaxosState = paxosState.commit(request.generation, Optional.ofNullable(request.committedValue));
        paxosLog.put(request.index, committedPaxosState);
        addAndApplyIfAllThePreviousEntriesAreCommitted(request);
        sendOneway(message.getFromAddress(), new CommitResponse(true), message.getCorrelationId());
    }

    private void addAndApplyIfAllThePreviousEntriesAreCommitted(CommitRequest commitRequest) {
        //if all entries upto logIndex - 1 are committed, apply this entry.
        List<Integer> previousIndexes = this.paxosLog.keySet().stream().filter(index -> index < commitRequest.index).collect(Collectors.toList());
        boolean allPreviousCommitted = true;
        for (Integer previousIndex : previousIndexes) {
            if (paxosLog.get(previousIndex).committedValue().isEmpty()) {
                allPreviousCommitted = false;
                break;
            }
        }
        if (allPreviousCommitted) {
            addAndApply(commitRequest.index, commitRequest.committedValue);
        }

        //see if there are entries above this logIndex which are committed, apply those entries.
        for (int startIndex = commitRequest.index + 1; ; startIndex++) {
            PaxosState paxosState = paxosLog.get(startIndex);
            if (paxosState == null || paxosState.committedValue().isEmpty()) {
                break;
            }
            byte[] committed = paxosState.committedValue().get();
            addAndApply(startIndex, committed);
        } //convert to streaming..
    }

    private void addAndApply(int index, byte[] walEnty) {
        Command command = Command.deserialize(walEnty);
        if (command instanceof SetValueCommand) {
            SetValueCommand setValueCommand = (SetValueCommand) command;
            kv.put(setValueCommand.getKey(), setValueCommand.getValue());
            requestWaitingList.handleResponse(index, new ExecuteCommandResponse(Optional.of(setValueCommand.getValue()), true));

        }
    }

    private void handlePaxosProposal(Message<ProposalRequest> message) {
        var request = message.messagePayload();
        var generation = request.generation;
        var paxosState = getOrCreatePaxosState(request.index);
        boolean accepted = false;
        if (generation.equals(fullLogBallot) || generation.isAfter(fullLogBallot)) {
            fullLogBallot = generation; //if its after the promisedBallot, update promisedBallot
            PaxosState acceptedPaxosState = paxosState.accept(request.generation, Optional.ofNullable(request.proposedValue));
            paxosLog.put(request.index, acceptedPaxosState);
            accepted = true;
        }
        sendOneway(message.getFromAddress(), new ProposalResponse(accepted), message.getCorrelationId());
    }

    MonotonicId fullLogBallot = MonotonicId.empty();

    private void handleFullLogPrepare(Message<PrepareRequest> message) {
        var request = message.messagePayload();
        MonotonicId ballot = request.monotonicId;
        if (fullLogBallot.isAfter(ballot)) {
            sendOneway(message.getFromAddress(), new FullLogPrepareResponse(false, Collections.EMPTY_MAP), message.getCorrelationId());
            return;
        }
        logger.info(getName() + " accepting ballot " + ballot + ". Becoming follower.");
        fullLogBallot = ballot;
        this.role = ServerRole.Follower;
        heartBeatScheduler.stop();
        heartbeatChecker.start();
        sendOneway(message.getFromAddress(), new FullLogPrepareResponse(true, getUncommitedValues()), message.getCorrelationId());
    }


    private Map<Integer, PaxosState> getUncommitedValues() {
        Map<Integer, PaxosState> uncommittedEntries = new HashMap<>();
        Set<Integer> indexes = paxosLog.keySet();
        for (Integer index : indexes) {
            PaxosState paxosState = paxosLog.get(index);
            if (paxosState.committedValue().isEmpty()) {
                uncommittedEntries.put(index, paxosState);
            }
        }
        return uncommittedEntries;
    }

    private PaxosState getOrCreatePaxosState(int index) {
        PaxosState paxosState = paxosLog.get(index);
        if (paxosState == null) {
            paxosState = new PaxosState();
            paxosLog.put(index, paxosState);
        }
        return paxosState;
    }

    public String getValue(String title) {
        return kv.get(title);
    }

    public boolean isLeader() {
        return role == ServerRole.Leader;
    }
}
