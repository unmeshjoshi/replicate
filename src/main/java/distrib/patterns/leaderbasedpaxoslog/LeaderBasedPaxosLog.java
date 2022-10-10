package distrib.patterns.leaderbasedpaxoslog;

import distrib.patterns.common.*;
import distrib.patterns.leaderbasedpaxoslog.messages.FullLogPrepareResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.common.MonotonicId;
import distrib.patterns.paxos.messages.CommitResponse;
import distrib.patterns.paxos.messages.GetValueResponse;
import distrib.patterns.paxos.messages.ProposalResponse;
import distrib.patterns.paxoslog.PaxosResult;
import distrib.patterns.paxoslog.messages.PrepareRequest;
import distrib.patterns.paxoslog.messages.ProposalRequest;
import distrib.patterns.paxoslog.messages.CommitRequest;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueResponse;
import distrib.patterns.twophasecommit.messages.ExecuteCommandRequest;
import distrib.patterns.twophasecommit.messages.ExecuteCommandResponse;
import distrib.patterns.vsr.CompletionCallback;
import distrib.patterns.wal.Command;
import distrib.patterns.wal.EntryType;
import distrib.patterns.wal.SetValueCommand;
import distrib.patterns.wal.WALEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class LeaderBasedPaxosLog extends Replica {
    private static Logger logger = LogManager.getLogger(LeaderBasedPaxosLog.class);
    private final SetValueCommand NO_OP_COMMAND = new SetValueCommand("", "");
    //Paxos State
    Map<Integer, PaxosState> paxosLog = new HashMap<>();

    Map<String, String> kv = new HashMap<>();

    public LeaderBasedPaxosLog(String name, SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        super(name, config, clock, clientAddress, peerConnectionAddress, peers);
        requestWaitingList = new RequestWaitingList(clock);
    }

    @Override
    protected void registerHandlers() {
        //client rpc
        handlesRequestAsync(RequestId.ExcuteCommandRequest, this::handleClientExecuteCommand, ExecuteCommandRequest.class);
        handlesRequestAsync(RequestId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);

        //peer to peer message passing
        handlesMessage(RequestId.Prepare, this::fullLogPrepare, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, FullLogPrepareResponse.class);

        handlesMessage(RequestId.ProposeRequest, this::handlePaxosProposal, ProposalRequest.class)
                .respondsWithMessage(RequestId.ProposeResponse, ProposalResponse.class);

        handlesMessage(RequestId.Commit, this::handlePaxosCommit, CommitRequest.class)
                .respondsWithMessage(RequestId.CommitResponse,  CommitResponse.class);
    }


    private CompletableFuture<ExecuteCommandResponse> handleClientExecuteCommand(ExecuteCommandRequest t) {
        var callback = new CompletionCallback<ExecuteCommandResponse>();
        //todo append should be async.
        append(new WALEntry(0l, t.command, EntryType.DATA, 0), callback);

        return callback.getFuture();
    }

    private CompletableFuture<GetValueResponse> handleClientGetValueRequest(GetValueRequest request) {
        var callback = new CompletionCallback<ExecuteCommandResponse>();

        append(new WALEntry(0l, NO_OP_COMMAND.serialize(), EntryType.DATA, 0), callback);
        return callback.getFuture().thenApply(r -> {
            return new GetValueResponse(Optional.ofNullable(kv.get(request.getKey())));
        });
    }

    RequestWaitingList requestWaitingList;

    int maxKnownPaxosRoundId = 1;
    int logIndex = 0;
    int serverId = 1;
    int maxAttempts = 2;

    public CompletableFuture<PaxosResult> append(WALEntry initialValue, CompletionCallback<ExecuteCommandResponse> callback) {
        return doPaxos(initialValue, callback);
    }

    ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();
    private CompletableFuture<PaxosResult> doPaxos(WALEntry value, CompletionCallback<ExecuteCommandResponse> callback) {
        int maxAttempts = 5;
        return FutureUtils.retryWithRandomDelay(() -> {
            //Each retry with higher generation/epoch
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            CompletableFuture<PaxosResult> result = doPaxos(monotonicId, logIndex, value, callback);
            logIndex++;//Increment so that next attempt is done for higher index.
            return result;
        }, maxAttempts, retryExecutor);

    }

    private CompletableFuture<PaxosResult> doPaxos(MonotonicId monotonicId, int index, WALEntry initialValue, CompletionCallback<ExecuteCommandResponse> callback) {
        return sendProposeRequest(index, initialValue, monotonicId)
                .thenCompose(proposedValue -> {
                    //Once the index at which the command is committed reaches 'high-watemark', return the result.
                    requestWaitingList.add(index, callback);
                    return sendCommitRequest(index, proposedValue, monotonicId)
                            .thenApply(r -> new PaxosResult(Optional.of(proposedValue), true));
                });
    }


    private CompletableFuture<Boolean> sendCommitRequest(int index, WALEntry value, MonotonicId monotonicId) {
        AsyncQuorumCallback<CommitResponse> commitCallback = new AsyncQuorumCallback<CommitResponse>(getNoOfReplicas(), c -> c.success);
        sendMessageToReplicas(commitCallback, RequestId.Commit, new CommitRequest(index, value, monotonicId));
        return commitCallback.getQuorumFuture().thenApply(result -> true);
    }


    private CompletableFuture<WALEntry> sendProposeRequest(int index, WALEntry proposedValue, MonotonicId monotonicId) {
        var proposalCallback = new AsyncQuorumCallback<ProposalResponse>(getNoOfReplicas(), p -> p.success);
        sendMessageToReplicas(proposalCallback, RequestId.ProposeRequest, new ProposalRequest(monotonicId, index, proposedValue));
        return proposalCallback.getQuorumFuture().thenApply(r -> proposedValue);
    }

    public void runElection() throws Exception {
        //TODO: convert to async
        this.fullLogPromisedGeneration = new MonotonicId(maxKnownPaxosRoundId++, serverId);
        var fullLogPrepareCallback = sendFullLogPrepare(fullLogPromisedGeneration);
        if (fullLogPrepareCallback.isQuorumPrepared()) {
            isLeader = true;
            List<FullLogPrepareResponse> promises = fullLogPrepareCallback.blockAndGetQuorumResponses().values().stream().toList();
            for (FullLogPrepareResponse promise : promises) {
                mergeLog(promise);
                sendProposalRequestsForUnCommittedEntries().get(); //wait for all the proposals to complete.
            }
        }
    }

    boolean isLeader = false;
    private CompletableFuture<Void> sendProposalRequestsForUnCommittedEntries() {
        List<CompletableFuture> commitFutures = new ArrayList<>();

        Map<Integer, PaxosState> uncommitedValues = getUncommitedValues();
        for (Integer index : uncommitedValues.keySet()) {
            PaxosState logEntry = uncommitedValues.get(index);
            WALEntry proposedValue = logEntry.acceptedValue.get();
            var completeFuture = sendProposeRequest(index, proposedValue, fullLogPromisedGeneration)
                    .thenCompose(value -> {
                        return sendCommitRequest(index, proposedValue, fullLogPromisedGeneration);
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
            if (selfEntry == null || isAfter(peerEntry.acceptedGeneration, selfEntry.acceptedGeneration)) {
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

    private FullLogPrepareCallback sendFullLogPrepare(MonotonicId fullLogPromisedGeneration) {
        var prepareCallback = new FullLogPrepareCallback(getNoOfReplicas());
        sendMessageToReplicas(prepareCallback, RequestId.Prepare, new PrepareRequest(-1, fullLogPromisedGeneration));
        return prepareCallback;
    }

    private CommitResponse handlePaxosCommit(CommitRequest request) {
        var paxosState = getOrCreatePaxosState(request.index);
        paxosState.committedGeneration = Optional.of(request.monotonicId);
        paxosState.committedValue = Optional.of(request.proposedValue);
        addAndApplyIfAllThePreviousEntriesAreCommitted(request);
        return new CommitResponse(true);
    }

    private void addAndApplyIfAllThePreviousEntriesAreCommitted(CommitRequest commitRequest) {
        //if all entries upto logIndex - 1 are committed, apply this entry.
        List<Integer> previousIndexes = this.paxosLog.keySet().stream().filter(index -> index < commitRequest.index).collect(Collectors.toList());
        boolean allPreviousCommitted = true;
        for (Integer previousIndex : previousIndexes) {
            if (paxosLog.get(previousIndex).committedValue.isEmpty()) {
                allPreviousCommitted = false;
                break;
            }
        }
        if (allPreviousCommitted) {
            addAndApply(commitRequest.index, commitRequest.proposedValue);
        }

        //see if there are entries above this logIndex which are commited, apply those entries.
        for(int startIndex = commitRequest.index + 1; ;startIndex++) {
            PaxosState paxosState = paxosLog.get(startIndex);
            if (paxosState == null) {
                break;
            }
            WALEntry committed = paxosState.committedValue.get();
            addAndApply(startIndex, committed);
        }
    }

    private void addAndApply(int index, WALEntry walEnty) {
        Command command = Command.deserialize(new ByteArrayInputStream(walEnty.getData()));
        if (command instanceof SetValueCommand) {
            SetValueCommand setValueCommand = (SetValueCommand)command;
            kv.put(setValueCommand.getKey(), setValueCommand.getValue());
            requestWaitingList.handleResponse(index, new ExecuteCommandResponse(Optional.of(setValueCommand.getValue()), true));

        }
    }

    private ProposalResponse handlePaxosProposal(ProposalRequest request) {
        var generation = request.generation;
        var paxosState = getOrCreatePaxosState(request.index);
        if (generation.equals(paxosState.promisedGeneration) || generation.isAfter(paxosState.promisedGeneration)) {
            paxosState.promisedGeneration = generation;
            paxosState.acceptedGeneration = Optional.of(generation);
            paxosState.acceptedValue = Optional.ofNullable(request.proposedValue);
            return new ProposalResponse(true);
        }
        return new ProposalResponse(false);
    }

    MonotonicId fullLogPromisedGeneration = MonotonicId.empty();

    private FullLogPrepareResponse fullLogPrepare(PrepareRequest request) {
        MonotonicId generation = request.monotonicId;
        if (fullLogPromisedGeneration.isAfter(generation)) {
            return new FullLogPrepareResponse(false, Collections.EMPTY_MAP);
        }
        fullLogPromisedGeneration = generation;
        return new FullLogPrepareResponse(true, getUncommitedValues());
    }


    private Map<Integer, PaxosState> getUncommitedValues() {
        Map<Integer, PaxosState> uncommittedEntries = new HashMap<>();
        Set<Integer> indexes = paxosLog.keySet();
        for (Integer index : indexes) {
            PaxosState paxosState = paxosLog.get(index);
            if (paxosState.committedValue.isPresent()) {
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
}
