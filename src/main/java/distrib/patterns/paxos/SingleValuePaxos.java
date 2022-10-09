package distrib.patterns.paxos;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.paxos.messages.*;
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

/**
 * Paxos operates in three phases
 * Prepare      : To order the request by assigning a unique epoch/generation number
 *                And to know about accepted values/Commands in previous quorum.
 * Propose      : Propose a new/selected value to all the nodes.
 * Commit(Learn): Tell all the nodes about the value/command accepted by majority quorum.
 *                Once committed the value can be returned to the user.
 *
 * +-------+         +--------+                 +-------+         +------+
 * |       |         |        |                 |       |         |      |
 * |Client |         |node1   |                 | node2 |         | node3|
 * |       |         |        |                 |       |         |      |
 * +---+---+         +----+---+                 +---+---+         +--+---+
 *     |   command        |                         |                |
 *     +------------------>                         |                |
 *     |                  +---+                     |                |
 *     |                  <---|     Prepare         |                |
 *     |                  +------------------------>+                |
 *     |                  | <-----------------------|                |
 *     |                  +----------------------------------------->+
 *     |                  <------------------------------------------+
 *     |                  |-----|                   |                |
 *     |                  <-----|   Propose         |                |
 *     |                  +------------------------>+               |
 *     |                  |<------------------------|                |
 *     |                  +----------------------------------------->+
 *     |                  +<-----------------------------------------+
 *     |                  |                         |                |
 *     |                  +------+                  |                |
 *     |                  |      |  Commit          |                |
 *     |                  <------+  Execute         |                |
 *     |                  |------------------------>+                |
 *     |   Result         +----------------------------------------> ++
 *     +<-----------------+                                          |
 *     |                  |                                          |
 *     +                  +                                          +
 */

public class SingleValuePaxos extends Replica {
    private static Logger logger = LogManager.getLogger(SingleValuePaxos.class);

    int maxKnownPaxosRoundId = 1;
    int serverId;
    int maxAttempts = 2;

    //Paxos State
    //TODO:Refactor so that all implementations have the same state representation.
    MonotonicId promisedGeneration = MonotonicId.empty();
    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<String> acceptedValue = Optional.empty();

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
        handlesMessage(RequestId.Prepare, this::prepare, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, PrepareResponse.class);

        handlesMessage(RequestId.ProposeRequest, this::handlePaxosProposal, ProposalRequest.class)
                .respondsWithMessage(RequestId.ProposeResponse, ProposalResponse.class);

        handlesMessage(RequestId.Commit, this::handlePaxosCommit, CommitRequest.class)
                .respondsWithMessage(RequestId.CommitResponse, CommitResponse.class);
    }

    private CompletableFuture<SetValueResponse> handleSetValueRequest(SetValueRequest setValueRequest) {
        return doPaxos(setValueRequest.getValue()).thenApply(value -> new SetValueResponse(value.orElse("")));
    }

    private CompletableFuture<GetValueResponse> handleGetValueRequest(GetValueRequest getValueRequest) {
        return doPaxos(null).thenApply(value -> new GetValueResponse(value));
    }

    private CommitResponse handlePaxosCommit(CommitRequest req) {
        if (req.getId().isAfter(promisedGeneration)) {
            this.acceptedValue = Optional.of(req.getValue());
            return new CommitResponse(true);
        }
        return new CommitResponse(false);
    }

    ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();

    private CompletableFuture<Optional<String>> doPaxos(String value) {
        int maxAttempts = 5;
        MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
        return FutureUtils.retryWithRandomDelay(() -> {
            return doPaxos(monotonicId, value);
        }, maxAttempts, retryExecutor).thenApply(result -> result.value);

    }

    static class PaxosResult {
        Optional<String> value;
        boolean success;

        public PaxosResult(Optional<String> value, boolean success) {
            this.value = value;
            this.success = success;
        }
    }

    private CompletableFuture<PaxosResult> doPaxos(MonotonicId monotonicId, String value) {
        AsyncQuorumCallback<PrepareResponse> prepareCallback = sendPrepareRequest(value, monotonicId);
        return prepareCallback.getQuorumFuture().thenCompose((result)->{
            String proposedValue = getProposalValue(value, result.values());
            return sendProposeRequest(proposedValue, monotonicId);
        }).thenApply(result -> {
            sendCommitRequest(monotonicId, value);
            return new PaxosResult(Optional.of(value), true);
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

    private BlockingQuorumCallback sendCommitRequest(MonotonicId monotonicId, String value) {
        BlockingQuorumCallback commitCallback = new BlockingQuorumCallback<>(getNoOfReplicas());
        sendMessageToReplicas(commitCallback, RequestId.Commit, new CommitRequest(monotonicId, value));
        return commitCallback;
    }

    private CompletableFuture<String> sendProposeRequest(String proposedValue, MonotonicId monotonicId) {
        AsyncQuorumCallback<ProposalResponse> proposalCallback = new AsyncQuorumCallback(getNoOfReplicas());
        sendMessageToReplicas(proposalCallback, RequestId.ProposeRequest, new ProposalRequest(monotonicId, proposedValue));
        return proposalCallback.getQuorumFuture().thenApply(result -> proposedValue);
    }

    public static class PrepareCallback extends BlockingQuorumCallback<PrepareResponse> {
        private String proposedValue;

        public PrepareCallback(String proposedValue, int clusterSize) {
            super(clusterSize);
            this.proposedValue = proposedValue;
        }

        public String getProposedValue() {
            return getProposalValue(proposedValue, responses.values());
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

        public boolean isQuorumPrepared() {
            return blockAndGetQuorumResponses()
                    .values()
                    .stream()
                    .filter(p -> p.promised).count() >= quorum;
        }
    }

    private AsyncQuorumCallback sendPrepareRequest(String proposedValue, MonotonicId monotonicId) {
        var callback = new AsyncQuorumCallback<PrepareResponse>(getNoOfReplicas(), p -> p.promised);
        sendMessageToReplicas(callback, RequestId.Prepare, new PrepareRequest(monotonicId));
        return callback;
    }

    private ProposalResponse handlePaxosProposal(ProposalRequest request) {
        MonotonicId generation = request.getMonotonicId();
        if (generation.equals(promisedGeneration) || generation.isAfter(promisedGeneration)) {
            this.promisedGeneration = generation;
            this.acceptedGeneration = Optional.of(generation);
            this.acceptedValue = Optional.ofNullable(request.getProposedValue());
            return new ProposalResponse(true);
        }
        return new ProposalResponse(false);
    }

    public PrepareResponse prepare(PrepareRequest prepareRequest) {
        MonotonicId generation = prepareRequest.monotonicId;
        if (promisedGeneration.isAfter(generation)) {
            return new PrepareResponse(false, acceptedValue, acceptedGeneration);
        }
        promisedGeneration = generation;
        return new PrepareResponse(true, acceptedValue, acceptedGeneration);
    }
}
