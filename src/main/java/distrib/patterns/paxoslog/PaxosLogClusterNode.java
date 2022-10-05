package distrib.patterns.paxoslog;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.paxos.*;
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

public class PaxosLogClusterNode extends Replica {
    private static Logger logger = LogManager.getLogger(PaxosLogClusterNode.class);

    //Paxos State
    Map<Integer, PaxosState> paxosLog = new HashMap<>();

    Map<String, String> kv = new HashMap<>();
    private SetValueCommand NO_OP_COMMAND = new SetValueCommand("", "");

    RequestWaitingList waitingList = new RequestWaitingList(clock);

    public PaxosLogClusterNode(String name, SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        super(name, config, clock, clientAddress, peerConnectionAddress, peers);
        requestWaitingList = new RequestWaitingList(clock);
    }


    @Override
    protected void registerHandlers() {
        //client rpc
        handlesRequestAsync(RequestId.SetValueRequest, this::handleClientSetValueRequest, SetValueRequest.class);
        handlesRequestAsync(RequestId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);

        //peer to peer message passing
        handlesMessage(RequestId.PrepareRequest, this::prepare, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, PrepareResponse.class);

        handlesMessage(RequestId.ProposeRequest, this::handlePaxosProposal, ProposalRequest.class)
                .respondsWithMessage(RequestId.ProposeResponse, ProposalResponse.class);

        handlesMessage(RequestId.CommitRequest, this::handlePaxosCommit, CommitRequest.class)
                .respondsWithMessage(RequestId.CommitResponse, CommitResponse.class);
    }


    private CompletableFuture<SetValueResponse> handleClientSetValueRequest(SetValueRequest request) {
        var callback = new CompletionCallback<SetValueResponse>();
        waitingList.add(logIndex, callback);

        SetValueCommand setValueCommand = new SetValueCommand(request.getKey(), request.getValue());
        //todo append should be async.
        int logIndex = append(new WALEntry(0l, setValueCommand.serialize(), EntryType.DATA, 0));

        return callback.getFuture();
    }

    private CompletableFuture<GetValueResponse> handleClientGetValueRequest(GetValueRequest request) {
        var callback = new CompletionCallback<SetValueResponse>();
        waitingList.add(logIndex, callback);

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

    private PaxosResult doPaxos(MonotonicId monotonicId, int index, WALEntry command) {
        PrepareCallback prepareCallback = sendPrepareRequest(index , command, monotonicId);
        if (prepareCallback.isQuorumPrepared()) {
            var proposedValue = prepareCallback.getProposedValue();
            var proposalCallback = sendProposeRequest(index, proposedValue, monotonicId);
            if (proposalCallback.isQuorumAccepted()) {
                sendCommitRequest(index, proposedValue, monotonicId);
                return new PaxosResult(Optional.ofNullable(proposedValue), true);
            }
        }
        return new PaxosResult(Optional.empty(), false);
    }


    private BlockingQuorumCallback sendCommitRequest(int index, WALEntry value, MonotonicId monotonicId) {
        var commitCallback = new BlockingQuorumCallback<>(getNoOfReplicas());
        sendMessageToReplicas(commitCallback, RequestId.CommitRequest, new CommitRequest(index, value, monotonicId));
        return commitCallback;
    }


    private distrib.patterns.paxos.ProposalCallback sendProposeRequest(int index, WALEntry proposedValue, MonotonicId monotonicId) {
        var proposalCallback = new distrib.patterns.paxos.ProposalCallback(getNoOfReplicas());
        sendMessageToReplicas(proposalCallback, RequestId.ProposeRequest, new ProposalRequest(monotonicId, index, proposedValue));
        return proposalCallback;
    }

    public static class PrepareCallback extends BlockingQuorumCallback<PrepareResponse> {
        private WALEntry proposedValue;

        public PrepareCallback(WALEntry proposedValue, int clusterSize) {
            super(clusterSize);
            this.proposedValue = proposedValue;
        }

        public WALEntry getProposedValue() {
            return getProposalValue(proposedValue, responses.values().stream().toList()); //TODO::
        }

        private WALEntry getProposalValue(WALEntry initialValue, List<PrepareResponse> promises) {
            var mostRecentAcceptedValue = getMostRecentAcceptedValue(promises);
            var proposedValue
                    = mostRecentAcceptedValue.acceptedValue.isEmpty() ?
                    initialValue : mostRecentAcceptedValue.acceptedValue.get();
            return proposedValue;
        }

        private PrepareResponse getMostRecentAcceptedValue(List<PrepareResponse> prepareResponses) {
            return prepareResponses.stream().max(Comparator.comparing(r -> r.acceptedGeneration.orElse(MonotonicId.empty()))).get();
        }

        public boolean isQuorumPrepared() {
            return blockAndGetQuorumResponses()
                    .values()
                    .stream()
                    .filter(p -> p.promised).count() >= quorum;
        }
    }

    private PrepareCallback sendPrepareRequest(int index, WALEntry proposedValue, MonotonicId monotonicId) {
        var prepareCallback = new PrepareCallback(proposedValue, getNoOfReplicas());
        sendMessageToReplicas(prepareCallback, RequestId.PrepareRequest, new PrepareRequest(index, monotonicId));
        return prepareCallback;
    }

    private distrib.patterns.paxos.CommitResponse handlePaxosCommit(CommitRequest request) {
        var paxosState = getOrCreatePaxosState(request.index);
        paxosState.committedGeneration = Optional.of(request.monotonicId);
        paxosState.committedValue = Optional.of(request.proposedValue);
        addAndApplyIfAllThePreviousEntriesAreCommitted(request);
        return new distrib.patterns.paxos.CommitResponse(true);
    }

    private void addAndApplyIfAllThePreviousEntriesAreCommitted(CommitRequest commitRequest) {
        //if all entries upto logIndex - 1 are committed, apply this entry.
        var previousIndexes = this.paxosLog.keySet().stream().filter(index -> index < commitRequest.index).collect(Collectors.toList());
        var allPreviousCommitted = true;
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
            var paxosState = paxosLog.get(startIndex);
            if (paxosState == null) {
                break;
            }
            var committed = paxosState.committedValue.get();
            addAndApply(startIndex, committed);
        }
    }

    private void addAndApply(int index, WALEntry walEnty) {
        var command = Command.deserialize(new ByteArrayInputStream(walEnty.getData()));
        if (command instanceof SetValueCommand) {
            SetValueCommand setValueCommand = (SetValueCommand)command;
            kv.put(setValueCommand.getKey(), setValueCommand.getValue());
            waitingList.handleResponse(index, new SetValueResponse(setValueCommand.getValue()));
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


    public PrepareResponse prepare(PrepareRequest request) {
        var paxosState = getOrCreatePaxosState(request.index);
        if (paxosState.promisedGeneration.isAfter(request.monotonicId)) {
            return new PrepareResponse(false, paxosState.acceptedValue, paxosState.acceptedGeneration);
        }
        paxosState.promisedGeneration = request.monotonicId;
        return new PrepareResponse(true, paxosState.acceptedValue, paxosState.acceptedGeneration);
    }

    private PaxosState getOrCreatePaxosState(int index) {
        var paxosState = paxosLog.get(index);
        if (paxosState == null) {
            paxosState = new PaxosState();
            paxosLog.put(index, paxosState);
        }
        return paxosState;
   }
}
