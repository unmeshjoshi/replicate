package replicate.paxoslog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.net.requestwaitinglist.RequestWaitingList;
import replicate.paxos.messages.CommitResponse;
import replicate.paxos.messages.GetValueResponse;
import replicate.paxos.messages.ProposalResponse;
import replicate.paxoslog.messages.CommitRequest;
import replicate.paxoslog.messages.PrepareRequest;
import replicate.paxoslog.messages.PrepareResponse;
import replicate.paxoslog.messages.ProposalRequest;
import replicate.quorum.messages.GetValueRequest;
import replicate.twophaseexecution.CompareAndSwap;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;
import replicate.twophaseexecution.messages.ExecuteCommandResponse;
import replicate.vsr.CompletionCallback;
import replicate.wal.Command;
import replicate.wal.SetValueCommand;

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
    int serverId;
    RequestWaitingList requestWaitingList;
    public PaxosLog(String name, SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        super(name, config, clock, clientAddress, peerConnectionAddress, peers);
        this.serverId = config.getServerId();
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
        var commitCallback = new CompletionCallback<ExecuteCommandResponse>();

        CompletableFuture<PaxosResult> appendFuture = append(t.command, commitCallback);

        return appendFuture.thenCompose(f -> commitCallback.getFuture());
    }


    private CompletableFuture<GetValueResponse> handleClientGetValueRequest(GetValueRequest request) {
        var commitCallback = new CompletionCallback<ExecuteCommandResponse>();
        var appendFuture = append(NO_OP_COMMAND.serialize(), commitCallback);
        return appendFuture
                .thenCompose(f ->
                        commitCallback.getFuture()
                                .thenApply(r -> new GetValueResponse(Optional.ofNullable(kv.get(request.getKey())))));
    }



    int maxKnownPaxosRoundId = 1;
    int logIndex = 0;

    public CompletableFuture<PaxosResult> append(byte[] initialValue, CompletionCallback<ExecuteCommandResponse> callback) {
        CompletableFuture<PaxosResult> appendFuture = doPaxos(initialValue, callback);
        return appendFuture.thenCompose((result)->{
           if (result.value.stream().allMatch(v -> v != initialValue)) {
               logger.info("Could not append proposed value to " + logIndex + ". Trying next index");
               logIndex = logIndex + 1;
               return append(initialValue, callback);
           }
           return CompletableFuture.completedFuture(result);
        });
    }

    ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();
    private CompletableFuture<PaxosResult> doPaxos(byte[] value, CompletionCallback<ExecuteCommandResponse> callback) {
        int maxAttempts = 2;
        return FutureUtils.retryWithRandomDelay(() -> {
            //Each retry with higher generation/epoch
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            CompletableFuture<PaxosResult> result = doPaxos(monotonicId, logIndex, value, callback);
            return result;
        }, maxAttempts, retryExecutor);
    }

    private CompletableFuture<PaxosResult> doPaxos(MonotonicId monotonicId, int index, byte[] initialValue, CompletionCallback<ExecuteCommandResponse> callback) {
        return sendPrepareRequest(index, monotonicId).
                thenCompose((result) -> {
                    byte[] proposedValue = getProposalValue(index, initialValue, result.values());
                    logger.debug(getName() + " proposing " + Command.deserialize(proposedValue) + " for index " + index + " Initial value is " + Command.deserialize(initialValue));
                    return sendProposeRequest(index, proposedValue, monotonicId);

                }).thenCompose(proposedValue -> {
                    //Once the index at which the command is committed reaches 'high-watemark', return the result.
                    if (proposedValue == initialValue) {
                        requestWaitingList.add(index, callback);
                    }
                    return sendCommitRequest(index, proposedValue, monotonicId)
                            .thenApply(r -> new PaxosResult(Optional.of(proposedValue), true));
                });
    }


    private byte[] getProposalValue(int index, byte[] initialValue, Collection<PrepareResponse> promises) {
        logger.debug(getName() + " got promises " + promises + " for index " + index);
        var mostRecentAcceptedValue = getMostRecentAcceptedValue(promises);
        return mostRecentAcceptedValue.acceptedValue.orElse(initialValue);
    }

    private PrepareResponse getMostRecentAcceptedValue(Collection<PrepareResponse> prepareResponses) {
        return prepareResponses.stream().max(Comparator.comparing(r -> r.acceptedGeneration.orElse(MonotonicId.empty()))).get();
    }

    private CompletableFuture<Boolean> sendCommitRequest(int index, byte[] value, MonotonicId monotonicId) {
        AsyncQuorumCallback<CommitResponse> commitCallback = new AsyncQuorumCallback<CommitResponse>(getNoOfReplicas(), c -> c.success);
        sendMessageToReplicas(commitCallback, RequestId.Commit, new CommitRequest(index, value, monotonicId));
        return commitCallback.getQuorumFuture().thenApply(result -> true);
    }


    private CompletableFuture<byte[]> sendProposeRequest(int index, byte[] proposedValue, MonotonicId monotonicId) {
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
        //Because commit is invoked only after successful prepare and propose. accept a commit message

        paxosState.committedGeneration = Optional.of(request.generation);
        paxosState.committedValue = Optional.of(request.committedValue);
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
            addAndApply(commitRequest.index, commitRequest.committedValue);
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

    private void addAndApply(int index, byte[] logEntry) {
        var command = Command.deserialize(logEntry);
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
        logger.info(getName() + " rejecting proposal " + request.generation + " as paxosState.promisedGeneration=" + paxosState.promisedGeneration);
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

    public String getValue(String title) {
        return kv.get(title);
    }
}
