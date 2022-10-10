package replicator.paxos;

import distrib.patterns.common.*;
import replicator.net.InetAddressAndPort;
import distrib.patterns.paxos.messages.*;
import replicator.quorum.messages.GetValueRequest;
import replicator.quorum.messages.SetValueRequest;
import replicator.quorum.messages.SetValueResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicator.common.*;
import replicator.paxos.messages.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Paxos operates in three phases
 * Prepare      : To order the request by assigning a unique epoch/generation number
 * And to know about accepted values/Commands in previous quorum.
 * Propose      : Propose a new/selected value to all the nodes.
 * Commit(Learn): Tell all the nodes about the value/command accepted by majority quorum.
 * Once committed the value can be returned to the user.
 * <p>
 * +-------+         +--------+                 +-------+         +------+
 * |       |         |        |                 |       |         |      |
 * |Client |         |node1   |                 | node2 |         | node3|
 * |       |         |        |                 |       |         |      |
 * +---+---+         +----+---+                 +---+---+         +--+---+
 * |   command        |                         |                |
 * +------------------>                         |                |
 * |                  +---+                     |                |
 * |                  <---|     Prepare         |                |
 * |                  +------------------------>+                |
 * |                  | <-----------------------|                |
 * |                  +----------------------------------------->+
 * |                  <------------------------------------------+
 * |                  |-----|                   |                |
 * |                  <-----|   Propose         |                |
 * |                  +------------------------>+               |
 * |                  |<------------------------|                |
 * |                  +----------------------------------------->+
 * |                  +<-----------------------------------------+
 * |                  |                         |                |
 * |                  +------+                  |                |
 * |                  |      |  Commit          |                |
 * |                  <------+  Execute         |                |
 * |                  |------------------------>+                |
 * |   Result         +----------------------------------------> ++
 * +<-----------------+                                          |
 * |                  |                                          |
 * +                  +                                          +
 */

public class SingleValuePaxos extends Replica {
    private static Logger logger = LogManager.getLogger(SingleValuePaxos.class);

    int maxKnownPaxosRoundId = 0;
    int serverId;

    //Paxos State
    //TODO:Refactor so that all implementations have the same state representation.
    public MonotonicId promisedGeneration = MonotonicId.empty();
    public Optional<MonotonicId> acceptedGeneration = Optional.empty();
    public Optional<String> acceptedValue = Optional.empty();

    Optional<String> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();

    public SingleValuePaxos(String name, SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        super(name, config, clock, clientAddress, peerConnectionAddress, peers);
        this.serverId = config.getServerId();
    }

    @Override
    protected void registerHandlers() {
        //client rpc
        handlesRequestAsync(RequestId.SetValueRequest, this::handleSetValueRequest, SetValueRequest.class)
                .respondsWith(RequestId.SetValueResponse, SetValueResponse.class);

        handlesRequestAsync(RequestId.GetValueRequest, this::handleGetValueRequest, GetValueRequest.class)
                .respondsWith(RequestId.GetValueRequest, GetValueResponse.class);

        //peer to peer message passing
        handlesMessage(RequestId.Prepare, this::handlePrepare, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, PrepareResponse.class);

        handlesMessage(RequestId.ProposeRequest, this::handleProposal, ProposalRequest.class)
                .respondsWithMessage(RequestId.ProposeResponse, ProposalResponse.class);

        handlesMessage(RequestId.Commit, this::handleCommit, CommitRequest.class)
                .respondsWithMessage(RequestId.CommitResponse, CommitResponse.class);
    }

    private CompletableFuture<SetValueResponse> handleSetValueRequest(SetValueRequest setValueRequest) {
        return doPaxos(setValueRequest.getValue()).thenApply(value -> new SetValueResponse(value.orElse("")));
    }

    private CompletableFuture<GetValueResponse> handleGetValueRequest(GetValueRequest getValueRequest) {
        return doPaxos(null).thenApply(value -> new GetValueResponse(value));
    }

    private CommitResponse handleCommit(CommitRequest req) {
        if (canAccept(req.getGeneration())) {
            logger.info("Accepting commit for " + req.getValue() + "promisedGeneration=" + promisedGeneration + " req generation=" + req.getGeneration());
            this.acceptedValue = Optional.ofNullable(req.getValue());
            return new CommitResponse(true);
        }
        return new CommitResponse(false);
    }

    ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();

    private CompletableFuture<Optional<String>> doPaxos(String value) {
        int maxAttempts = 2;
        return FutureUtils.retryWithRandomDelay(() -> {
            //Each retry with higher generation/epoch
            maxKnownPaxosRoundId = maxKnownPaxosRoundId + 1;
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId, serverId);
            return doPaxos(monotonicId, value);
        }, maxAttempts, retryExecutor).thenApply(result -> result.value);

    }

    public static class PaxosResult {
        public final Optional<String> value;
        public final boolean success;

        public PaxosResult(Optional<String> value, boolean success) {
            this.value = value;
            this.success = success;
        }
    }

    private CompletableFuture<PaxosResult> doPaxos(MonotonicId monotonicId, String value) {
        logger.info(getName() + ": Sending Prepare with " + monotonicId);
        var prepareFuture = sendPrepareRequest(monotonicId);
        return prepareFuture
                .thenCompose((result) -> {
                    String proposedValue = getProposalValue(value, result.values());
                    logger.info(getName() + ": Proposing " + proposedValue + " for generation " + monotonicId);
                    return sendProposeRequest(proposedValue, monotonicId);
                }).thenCompose(acceptedValue -> {
                    logger.info(getName() + ": Committing value " +  acceptedValue + " for generation " + monotonicId);
                    return sendCommitRequest(monotonicId, acceptedValue)
                            .thenApply(r -> new PaxosResult(Optional.ofNullable(acceptedValue), true));
                });
    }


    private String getProposalValue(String initialValue, Collection<PrepareResponse> promises) {
        PrepareResponse mostRecentAcceptedValue = getMostRecentAcceptedValue(promises);
        logger.debug("Most Recent promise " + mostRecentAcceptedValue);
        return mostRecentAcceptedValue.acceptedValue.orElse(initialValue);
    }

    private PrepareResponse getMostRecentAcceptedValue(Collection<PrepareResponse> prepareResponses) {
        logger.debug("Picking up values from " + prepareResponses);
        return prepareResponses.stream().max(Comparator.comparing(r -> r.acceptedGeneration.orElse(MonotonicId.empty()))).get();
    }

    private CompletableFuture<Boolean> sendCommitRequest(MonotonicId monotonicId, String value) {
        AsyncQuorumCallback<CommitResponse> commitCallback = new AsyncQuorumCallback<CommitResponse>(getNoOfReplicas(), c -> c.success);
        sendMessageToReplicas(commitCallback, RequestId.Commit, new CommitRequest(monotonicId, value));
        return commitCallback.getQuorumFuture().thenApply(r -> true);
    }

    private CompletableFuture<String> sendProposeRequest(String proposedValue, MonotonicId monotonicId) {
        AsyncQuorumCallback<ProposalResponse> proposalCallback = new AsyncQuorumCallback<ProposalResponse>(getNoOfReplicas(), p -> p.success);
        sendMessageToReplicas(proposalCallback, RequestId.ProposeRequest, new ProposalRequest(monotonicId, proposedValue));
        return proposalCallback.getQuorumFuture().thenApply(result -> proposedValue);
    }

    private CompletableFuture<Map<InetAddressAndPort, PrepareResponse>> sendPrepareRequest(MonotonicId monotonicId) {
        var callback = new AsyncQuorumCallback<PrepareResponse>(getNoOfReplicas(), p -> p.promised);
        sendMessageToReplicas(callback, RequestId.Prepare, new PrepareRequest(monotonicId));
        return callback.getQuorumFuture();
    }

    private ProposalResponse handleProposal(ProposalRequest request) {
        MonotonicId generation = request.getMonotonicId();
        if (canAccept(generation)) {
            this.promisedGeneration = generation;
            this.acceptedGeneration = Optional.of(generation);
            this.acceptedValue = Optional.ofNullable(request.getProposedValue());
            return new ProposalResponse(true);
        }
        return new ProposalResponse(false);
    }

    private boolean canAccept(MonotonicId generation) {
        return generation.equals(promisedGeneration) || generation.isAfter(promisedGeneration);
    }

    public PrepareResponse handlePrepare(PrepareRequest prepareRequest) {
        MonotonicId generation = prepareRequest.monotonicId;
        if (promisedGeneration.isAfter(generation)) {
            return new PrepareResponse(false, acceptedValue, acceptedGeneration);
        }
        promisedGeneration = generation;
        return new PrepareResponse(true, acceptedValue, acceptedGeneration);
    }
}
