package replicate.paxoskv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.paxos.PaxosState;
import replicate.paxos.SingleValuePaxos;
import replicate.paxos.messages.CommitResponse;
import replicate.paxos.messages.GetValueResponse;
import replicate.paxos.messages.PrepareResponse;
import replicate.paxos.messages.ProposalResponse;
import replicate.paxoskv.messages.CommitRequest;
import replicate.paxoskv.messages.PrepareRequest;
import replicate.paxoskv.messages.ProposalRequest;
import replicate.quorum.messages.GetValueRequest;
import replicate.quorum.messages.SetValueRequest;
import replicate.quorum.messages.SetValueResponse;
import replicate.wal.Command;
import replicate.wal.SetValueCommand;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

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
        handlesRequestAsync(MessageId.SetValueRequest, this::handleClientSetValueRequest, SetValueRequest.class);
        handlesRequestAsync(MessageId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);

        //peer to peer message passing
        handlesMessage(MessageId.Prepare, this::handlePaxosPrepare, PrepareRequest.class);
        handlesMessage(MessageId.Promise, this::handlePrepareResponse, PrepareResponse.class);

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

    private void handlePrepareResponse(Message<PrepareResponse> prepareResponseMessage) {
        handleResponse(prepareResponseMessage);
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
    private CompletableFuture<SingleValuePaxos.PaxosResult> doPaxos(String key, String value) {
        int maxAttempts = 5;
        return FutureUtils.retryWithRandomDelay(() -> {
            //Each retry with higher generation/epoch
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            return doPaxos(monotonicId, key, new SetValueCommand(key, value == null?"":value).serialize());
        }, maxAttempts, singularUpdateQueueExecutor);
    }

    private CompletableFuture<SingleValuePaxos.PaxosResult> doPaxos(MonotonicId monotonicId, String key, byte[] initialValue) {
        return sendPrepareRequest(key, monotonicId).
                thenCompose((result) -> {
                    byte[] proposedValue = getProposalValue(initialValue, result.values());
                    return sendProposeRequest(key, proposedValue, monotonicId);

                }).thenCompose(proposedValue -> {
                    return sendCommitRequest(key, proposedValue, monotonicId)
                            .thenApply(r -> {
                                String result = execute(proposedValue);
                                return new SingleValuePaxos.PaxosResult(Optional.ofNullable(result), true);
                            });
                });
    }

    private String execute(byte[] proposedValue) {
        Command command = Command.deserialize(new ByteArrayInputStream(proposedValue));
        //TODO.
        return ((SetValueCommand) command).getValue();
    }

    private byte[] getProposalValue(byte[] initialValue, Collection<PrepareResponse> promises) {
        PrepareResponse mostRecentAcceptedValue = getMostRecentAcceptedValue(promises);
        byte[] proposedValue
                = mostRecentAcceptedValue.acceptedValue.isEmpty() ?
                initialValue : mostRecentAcceptedValue.acceptedValue.get();
        return proposedValue;
    }


    private PrepareResponse getMostRecentAcceptedValue(Collection<PrepareResponse> prepareResponses) {
        return prepareResponses.stream().max(Comparator.comparing(r -> r.acceptedGeneration.orElse(MonotonicId.empty()))).get();
    }

    private CompletableFuture<Boolean> sendCommitRequest(String key, byte[] value, MonotonicId monotonicId) {
        AsyncQuorumCallback<CommitResponse> commitCallback = new AsyncQuorumCallback<CommitResponse>(getNoOfReplicas(), c -> c.success);
        sendMessageToReplicas(commitCallback, MessageId.Commit, new CommitRequest(key, value, monotonicId));
        return commitCallback.getQuorumFuture().thenApply(result -> true);
    }

    private CompletableFuture<byte[]> sendProposeRequest(String key, byte[] proposedValue, MonotonicId monotonicId) {
        AsyncQuorumCallback<ProposalResponse> proposalCallback = new AsyncQuorumCallback(getNoOfReplicas());
        sendMessageToReplicas(proposalCallback, MessageId.ProposeRequest, new ProposalRequest(monotonicId, key, proposedValue));
        return proposalCallback.getQuorumFuture().thenApply(result -> proposedValue);
    }


    private CompletableFuture<Map<InetAddressAndPort, PrepareResponse>> sendPrepareRequest(String key, MonotonicId monotonicId) {
        var callback = new AsyncQuorumCallback<PrepareResponse>(getNoOfReplicas(), p -> p.promised);
        sendMessageToReplicas(callback, MessageId.Prepare, new PrepareRequest(key, monotonicId));
        return callback.getQuorumFuture();
    }


    private void handlePaxosCommit(Message<CommitRequest> message) {
        var  request = message.messagePayload();
        PaxosState paxosState = getOrCreatePaxosState(request.key);

        //Because commit is invoked only after successful prepare and propose.
        paxosState.commit(request.generation, Optional.ofNullable(request.value));
        kv.put(request.key, paxosState);

        sendOneway(message.getFromAddress(), new CommitResponse(true), message.getCorrelationId());
    }

    private void handlePaxosProposal(Message<ProposalRequest> message) {
        var  request = message.messagePayload();
        var accepted = false;
        PaxosState paxosState = getOrCreatePaxosState(request.key);
        if (paxosState.canAccept(request.generation)) {
            paxosState = paxosState.accept(request.generation, Optional.ofNullable(request.proposedValue));
            kv.put(request.key, paxosState);
            accepted = true;
        }
        sendOneway(message.getFromAddress(), new ProposalResponse(accepted), message.getCorrelationId());
    }

    public void handlePaxosPrepare(Message<PrepareRequest> message) {
        var request = message.messagePayload();
        PaxosState paxosState = getOrCreatePaxosState(request.key);
        boolean promised = false;
        if (paxosState.canPromise(request.generation)) {
            paxosState = paxosState.promise(request.generation);
            kv.put(request.key, paxosState);
            promised = true;
        }
        sendOneway(message.getFromAddress(), new PrepareResponse(promised, paxosState.acceptedValue(), paxosState.acceptedBallot()), message.getCorrelationId());
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
