package distrib.patterns.paxoskv;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.paxos.*;
import distrib.patterns.paxos.messages.CommitResponse;
import distrib.patterns.paxos.messages.GetValueResponse;
import distrib.patterns.paxos.messages.PrepareResponse;
import distrib.patterns.paxos.messages.ProposalResponse;
import distrib.patterns.paxoskv.messages.CommitRequest;
import distrib.patterns.paxoskv.messages.PrepareRequest;
import distrib.patterns.paxoskv.messages.ProposalRequest;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueRequest;
import distrib.patterns.quorum.messages.SetValueResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class PaxosState {
    MonotonicId promisedGeneration = MonotonicId.empty();
    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<String> acceptedValue = Optional.empty();

    Optional<String> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();

}

public class PaxosKVStore extends Replica {
    private static Logger logger = LogManager.getLogger(PaxosKVStore.class);

    //Paxos State per key
    Map<String, PaxosState> kv = new HashMap<>();

    public PaxosKVStore(String name, SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        super(name, config, clock, clientAddress, peerConnectionAddress, peers);
    }



    @Override
    protected void registerHandlers() {
        //client rpc
        handlesRequestAsync(RequestId.SetValueRequest, this::handleClientSetValueRequest, SetValueRequest.class);
        handlesRequestAsync(RequestId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);

        //peer to peer message passing
        handlesMessage(RequestId.Prepare, this::prepare, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, PrepareResponse.class);

        handlesMessage(RequestId.ProposeRequest, this::handlePaxosProposal, ProposalRequest.class)
                .respondsWithMessage(RequestId.ProposeResponse, ProposalResponse.class);

        handlesMessage(RequestId.Commit, this::handlePaxosCommit, CommitRequest.class)
                .respondsWithMessage(RequestId.CommitResponse, CommitResponse.class);
    }

    private CompletableFuture<GetValueResponse> handleClientGetValueRequest(GetValueRequest request) {
        return doPaxos(request.getKey(), null).thenApply(result -> new GetValueResponse(result.value));
    }

    private CompletableFuture<SetValueResponse> handleClientSetValueRequest(SetValueRequest setValueRequest) {
        return doPaxos(setValueRequest.getKey(), setValueRequest.getValue())
                .thenApply(result -> new SetValueResponse(result.value.orElse("")));
    }

    int maxKnownPaxosRoundId = 1;
    int serverId = 1;
    ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();

    private CompletableFuture<SingleValuePaxos.PaxosResult> doPaxos(String key, String value) {
        int maxAttempts = 5;
        return FutureUtils.retryWithRandomDelay(() -> {
            //Each retry with higher generation/epoch
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            return doPaxos(monotonicId, key, value);
        }, maxAttempts, retryExecutor);

    }

    private CompletableFuture<SingleValuePaxos.PaxosResult> doPaxos(MonotonicId monotonicId, String key, String value) {
        return sendPrepareRequest(key, monotonicId).
                thenCompose((result) -> {
                    String proposedValue = getProposalValue(value, result.values());
                    return sendProposeRequest(key, proposedValue, monotonicId);

                }).thenCompose(proposedValue -> {
                    return sendCommitRequest(key, proposedValue, monotonicId)
                            .thenApply(r -> new SingleValuePaxos.PaxosResult(Optional.ofNullable(proposedValue), true));
                });
    }

    private String getProposalValue(String initialValue, Collection<PrepareResponse> promises) {
        PrepareResponse mostRecentAcceptedValue = getMostRecentAcceptedValue(promises);
        String proposedValue
                = mostRecentAcceptedValue.acceptedValue.isEmpty() ?
                initialValue : mostRecentAcceptedValue.acceptedValue.get();
        return proposedValue;
    }


    private PrepareResponse getMostRecentAcceptedValue(Collection<PrepareResponse> prepareResponses) {
        return prepareResponses.stream().max(Comparator.comparing(r -> r.acceptedGeneration.orElse(MonotonicId.empty()))).get();
    }

    private CompletableFuture<Boolean> sendCommitRequest(String key, String value, MonotonicId monotonicId) {
        AsyncQuorumCallback<CommitResponse> commitCallback = new AsyncQuorumCallback<CommitResponse>(getNoOfReplicas(), c -> c.success);
        sendMessageToReplicas(commitCallback, RequestId.Commit, new CommitRequest(key, value, monotonicId));
        return commitCallback.getQuorumFuture().thenApply(result -> true);
    }

    private CompletableFuture<String> sendProposeRequest(String key, String proposedValue, MonotonicId monotonicId) {
        AsyncQuorumCallback<ProposalResponse> proposalCallback = new AsyncQuorumCallback(getNoOfReplicas());
        sendMessageToReplicas(proposalCallback, RequestId.ProposeRequest, new ProposalRequest(monotonicId, key, proposedValue));
        return proposalCallback.getQuorumFuture().thenApply(result -> proposedValue);
    }


    private CompletableFuture<Map<InetAddressAndPort, PrepareResponse>> sendPrepareRequest(String key, MonotonicId monotonicId) {
        var callback = new AsyncQuorumCallback<PrepareResponse>(getNoOfReplicas(), p -> p.promised);
        sendMessageToReplicas(callback, RequestId.Prepare, new PrepareRequest(key, monotonicId));
        return callback.getQuorumFuture();
    }


    private CommitResponse handlePaxosCommit(CommitRequest request) {
        PaxosState paxosState = getOrCreatePaxosState(request.key);

        //Because commit is invoked only after successful prepare and propose.
        assert paxosState.promisedGeneration.equals(request.generation) || request.generation.isAfter(paxosState.promisedGeneration);

        paxosState.committedGeneration = Optional.of(request.generation);
        paxosState.committedValue = Optional.ofNullable(request.value);
        return new CommitResponse(true);
    }

    private ProposalResponse handlePaxosProposal(ProposalRequest request) {
        PaxosState paxosState = getOrCreatePaxosState(request.key);
        if (request.generation.equals(paxosState.promisedGeneration) || request.generation.isAfter(paxosState.promisedGeneration)) {
            paxosState.promisedGeneration = request.generation;
            paxosState.acceptedGeneration = Optional.of(request.generation);
            paxosState.acceptedValue = Optional.ofNullable(request.proposedValue);
            return new ProposalResponse(true);
        }
        return new ProposalResponse(false);
    }

    public PrepareResponse prepare(PrepareRequest request) {
        PaxosState paxosState = getOrCreatePaxosState(request.key);
        if (paxosState.promisedGeneration.isAfter(request.generation)) {
            return new PrepareResponse(false, paxosState.acceptedValue, paxosState.acceptedGeneration);
        }
        paxosState.promisedGeneration = request.generation;
        return new PrepareResponse(true, paxosState.acceptedValue, paxosState.acceptedGeneration);
    }

    private PaxosState getOrCreatePaxosState(String key) {
        PaxosState paxosState = kv.get(key);
        if (paxosState == null) {
            paxosState = new PaxosState();
            kv.put(key, paxosState);
        }
        return paxosState;
    }
}
