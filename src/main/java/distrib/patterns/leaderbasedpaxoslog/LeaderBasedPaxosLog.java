package distrib.patterns.leaderbasedpaxoslog;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.common.MonotonicId;
import distrib.patterns.paxos.GetValueResponse;
import distrib.patterns.paxos.ProposalResponse;
import distrib.patterns.paxos.WriteTimeoutException;
import distrib.patterns.paxoslog.PrepareRequest;
import distrib.patterns.paxoslog.ProposalRequest;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueRequest;
import distrib.patterns.quorum.messages.SetValueResponse;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class PaxosState {
    MonotonicId promisedGeneration = MonotonicId.empty();
    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<WALEntry> acceptedValue = Optional.empty();

    Optional<WALEntry> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();

}

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
        handlesRequestAsync(RequestId.SetValueRequest, this::handleClientSetValueRequest, SetValueRequest.class);
        handlesRequestAsync(RequestId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);

        //peer to peer message passing
        handlesMessage(RequestId.Prepare, this::fullLogPrepare, distrib.patterns.paxoslog.PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, FullLogPrepareResponse.class);

        handlesMessage(RequestId.ProposeRequest, this::handlePaxosProposal, distrib.patterns.paxoslog.ProposalRequest.class)
                .respondsWithMessage(RequestId.ProposeResponse, distrib.patterns.paxos.ProposalResponse.class);

        handlesMessage(RequestId.Commit, this::handlePaxosCommit, distrib.patterns.paxoslog.CommitRequest.class)
                .respondsWithMessage(RequestId.CommitResponse,  distrib.patterns.paxos.CommitResponse.class);
    }


    private CompletableFuture<SetValueResponse> handleClientSetValueRequest(SetValueRequest request) {
        var callback = new CompletionCallback<SetValueResponse>();
        requestWaitingList.add(logIndex, callback);

        SetValueCommand setValueCommand = new SetValueCommand(request.getKey(), request.getValue());
        //todo append should be async.
        int logIndex = append(new WALEntry(0l, setValueCommand.serialize(), EntryType.DATA, 0));

        return callback.getFuture();
    }

    private CompletableFuture<GetValueResponse> handleClientGetValueRequest(GetValueRequest request) {
        var callback = new CompletionCallback<SetValueResponse>();
        requestWaitingList.add(logIndex, callback);

        var logIndex = append(new WALEntry(0l, NO_OP_COMMAND.serialize(), EntryType.DATA, 0));
        return callback.getFuture().thenApply(r -> {
            return new GetValueResponse(Optional.ofNullable(kv.get(request.getKey())));
        });
    }

    RequestWaitingList requestWaitingList;

    int maxKnownPaxosRoundId = 1;
    int logIndex = 0;
    int serverId = 1;
    int maxAttempts = 2;
    //conver to async
    public int append(WALEntry initialValue) {
        int attempts = 0;
        while(attempts <= maxAttempts) {
            attempts++;
            var requestId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            var paxosResult = doPaxos(requestId, logIndex, initialValue);
            if (paxosResult.value.isPresent() && paxosResult.value.get() == initialValue) {
                return logIndex;
            }
            Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), MILLISECONDS);
            logger.warn("Experienced Paxos contention. Attempting with higher generation");
            logIndex++;
        }
        throw new WriteTimeoutException(attempts);
    }

    static class PaxosResult {
        Optional<WALEntry> value;
        boolean success;

        public PaxosResult(Optional<WALEntry> value, boolean success) {
            this.value = value;
            this.success = success;
        }
    }


    private PaxosResult doPaxos(MonotonicId monotonicId, int index, WALEntry proposedValue) {
        distrib.patterns.paxos.ProposalCallback proposalCallback = sendProposeRequest(index, proposedValue, monotonicId);
        if (proposalCallback.isQuorumAccepted()) {
            sendCommitRequest(index, proposedValue, monotonicId);
            return new PaxosResult(Optional.ofNullable(proposedValue), true);
        }
        return new PaxosResult(Optional.empty(), false);
    }


    private BlockingQuorumCallback sendCommitRequest(int index, WALEntry value, MonotonicId monotonicId) {
        var commitCallback = new BlockingQuorumCallback<>(getNoOfReplicas());
        sendMessageToReplicas(commitCallback, RequestId.Commit, new distrib.patterns.paxoslog.CommitRequest(index, value, monotonicId));
        return commitCallback;
    }


    private distrib.patterns.paxos.ProposalCallback sendProposeRequest(int index, WALEntry proposedValue, MonotonicId monotonicId) {
        var proposalCallback = new distrib.patterns.paxos.ProposalCallback(getNoOfReplicas());
        sendMessageToReplicas(proposalCallback, RequestId.ProposeRequest, new distrib.patterns.paxoslog.ProposalRequest(monotonicId, index, proposedValue));
        return proposalCallback;
    }

    public void runElection() {
        this.fullLogPromisedGeneration = new MonotonicId(maxKnownPaxosRoundId++, serverId);
        var fullLogPrepareCallback = sendFullLogPrepare(fullLogPromisedGeneration);
        if (fullLogPrepareCallback.isQuorumPrepared()) {
            isLeader = true;
            //TODO:
            List<FullLogPrepareResponse> promises = fullLogPrepareCallback.blockAndGetQuorumResponses().values().stream().toList();
            for (FullLogPrepareResponse promise : promises) {
                mergeLog(promise);
                sendProposalRequestsForUnCommittedEntries();
            }
        }
    }

    boolean isLeader = false;
    private void sendProposalRequestsForUnCommittedEntries() {
        Map<Integer, PaxosState> uncommitedValues = getUncommitedValues();
        for (Integer index : uncommitedValues.keySet()) {
            PaxosState logEntry = uncommitedValues.get(index);
            WALEntry proposedValue = logEntry.acceptedValue.get();
            distrib.patterns.paxos.ProposalCallback proposalCallback = sendProposeRequest(index, proposedValue, fullLogPromisedGeneration);
            if (!proposalCallback.isQuorumAccepted()) {
                isLeader = false;
            }
            sendCommitRequest(index, proposedValue, fullLogPromisedGeneration);
        }
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


    private distrib.patterns.paxos.CommitResponse handlePaxosCommit(distrib.patterns.paxoslog.CommitRequest request) {
        var paxosState = getOrCreatePaxosState(request.index);
        paxosState.committedGeneration = Optional.of(request.monotonicId);
        paxosState.committedValue = Optional.of(request.proposedValue);
        addAndApplyIfAllThePreviousEntriesAreCommitted(request);
        return new distrib.patterns.paxos.CommitResponse(true);
    }

    private void addAndApplyIfAllThePreviousEntriesAreCommitted(distrib.patterns.paxoslog.CommitRequest commitRequest) {
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
            requestWaitingList.handleResponse(index, new SetValueResponse(setValueCommand.getValue()));
        }
    }

    private distrib.patterns.paxos.ProposalResponse handlePaxosProposal(ProposalRequest request) {
        var generation = request.generation;
        var paxosState = getOrCreatePaxosState(request.index);
        if (generation.equals(paxosState.promisedGeneration) || generation.isAfter(paxosState.promisedGeneration)) {
            paxosState.promisedGeneration = generation;
            paxosState.acceptedGeneration = Optional.of(generation);
            paxosState.acceptedValue = Optional.ofNullable(request.proposedValue);
            return new distrib.patterns.paxos.ProposalResponse(true);
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
