package distrib.patterns.paxoslog;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.paxos.messages.CommitResponse;
import distrib.patterns.paxos.messages.GetValueResponse;
import distrib.patterns.paxos.messages.ProposalResponse;
import distrib.patterns.paxoslog.messages.CommitRequest;
import distrib.patterns.paxoslog.messages.PrepareRequest;
import distrib.patterns.paxoslog.messages.PrepareResponse;
import distrib.patterns.paxoslog.messages.ProposalRequest;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.twophasecommit.CompareAndSwap;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class PaxosLog extends Replica {
    private static Logger logger = LogManager.getLogger(PaxosLog.class);

    //Paxos State
    Map<Integer, PaxosState> paxosLog = new HashMap<>();

    Map<String, String> kv = new HashMap<>();
    private final SetValueCommand NO_OP_COMMAND = new SetValueCommand("", "");

    RequestWaitingList requestWaitingList;
    public PaxosLog(String name, SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        super(name, config, clock, clientAddress, peerConnectionAddress, peers);
        requestWaitingList = new RequestWaitingList(clock);
    }


    @Override
    protected void registerHandlers() {
        //client rpc
        handlesRequestAsync(RequestId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);
        handlesRequestAsync(RequestId.ExcuteCommandRequest, this::handleClientExecuteCommand, ExecuteCommandRequest.class);

        //peer to peer message passing
        handlesMessage(RequestId.Prepare, this::prepare, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, PrepareResponse.class);

        handlesMessage(RequestId.ProposeRequest, this::handlePaxosProposal, ProposalRequest.class)
                .respondsWithMessage(RequestId.ProposeResponse, ProposalResponse.class);

        handlesMessage(RequestId.Commit, this::handlePaxosCommit, CommitRequest.class)
                .respondsWithMessage(RequestId.CommitResponse, CommitResponse.class);
    }

    private CompletableFuture<ExecuteCommandResponse> handleClientExecuteCommand(ExecuteCommandRequest t) {
        var callback = new CompletionCallback<ExecuteCommandResponse>();
        //todo append should be async.
        append(new WALEntry(0l, t.command, EntryType.DATA, 0), callback);

        return callback.getFuture();
    }


    private CompletableFuture<GetValueResponse> handleClientGetValueRequest(GetValueRequest request) {
        var callback = new CompletionCallback<ExecuteCommandResponse>();
        var logIndex = append(new WALEntry(0l, NO_OP_COMMAND.serialize(), EntryType.DATA, 0), callback);
        return callback.getFuture().thenApply(r -> {
            return new GetValueResponse(Optional.ofNullable(kv.get(request.getKey())));
        });
    }



    int maxKnownPaxosRoundId = 1;
    int logIndex = 0;
    int serverId = 1;
    int maxAttempts = 2;

    //conver to async
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

    private CompletableFuture<PaxosResult> doPaxos(MonotonicId monotonicId, int index, WALEntry value, CompletionCallback<ExecuteCommandResponse> callback) {
        return sendPrepareRequest(index, monotonicId).
                thenCompose((result) -> {
                    WALEntry proposedValue = getProposalValue(value, result.values());
                    return sendProposeRequest(index, proposedValue, monotonicId);

                }).thenCompose(proposedValue -> {
                    //Once the index at which the command is committed reaches 'high-watemark', return the result.
                    requestWaitingList.add(index, callback);
                    return sendCommitRequest(index, proposedValue, monotonicId)
                            .thenApply(r -> new PaxosResult(Optional.of(proposedValue), true));
                });
    }


    private WALEntry getProposalValue(WALEntry initialValue, Collection<PrepareResponse> promises) {
        var mostRecentAcceptedValue = getMostRecentAcceptedValue(promises);
        var proposedValue
                = mostRecentAcceptedValue.acceptedValue.isEmpty() ?
                initialValue : mostRecentAcceptedValue.acceptedValue.get();
        return proposedValue;
    }

    private PrepareResponse getMostRecentAcceptedValue(Collection<PrepareResponse> prepareResponses) {
        return prepareResponses.stream().max(Comparator.comparing(r -> r.acceptedGeneration.orElse(MonotonicId.empty()))).get();
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

    private CompletableFuture<Map<InetAddressAndPort, PrepareResponse>> sendPrepareRequest(int index, MonotonicId monotonicId) {
        var callback = new AsyncQuorumCallback<PrepareResponse>(getNoOfReplicas(), p -> p.promised);
        sendMessageToReplicas(callback, RequestId.Prepare, new PrepareRequest(index, monotonicId));
        return callback.getQuorumFuture();
    }

    private CommitResponse handlePaxosCommit(CommitRequest request) {
        var paxosState = getOrCreatePaxosState(request.index);
        //Because commit is invoked only after successful prepare and propose.
        assert paxosState.promisedGeneration.equals(request.generation) || request.generation.isAfter(paxosState.promisedGeneration);

        paxosState.committedGeneration = Optional.of(request.generation);
        paxosState.committedValue = Optional.of(request.proposedValue);
        addAndApplyIfAllThePreviousEntriesAreCommitted(request);
        return new CommitResponse(true);
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
            requestWaitingList.handleResponse(index, new ExecuteCommandResponse(Optional.of(setValueCommand.getValue()), true));

        } else if (command instanceof CompareAndSwap) {
            CompareAndSwap cas = (CompareAndSwap)command;
            Optional<String> existingValue = Optional.ofNullable(kv.get(cas.getKey()));
            if (existingValue.equals(cas.getExistingValue())) {
                kv.put(cas.getKey(), cas.getNewValue());
                requestWaitingList.handleResponse(index,  new ExecuteCommandResponse(existingValue, true));
            }
            requestWaitingList.handleResponse(index,  new ExecuteCommandResponse(existingValue, false));
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
