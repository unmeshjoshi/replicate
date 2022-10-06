package distrib.patterns.paxos;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueRequest;
import distrib.patterns.quorum.messages.SetValueResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SingleValuePaxos extends Replica {
    private static Logger logger = LogManager.getLogger(SingleValuePaxos.class);

    int maxKnownPaxosRoundId = 1;
    int serverId;
    int maxAttempts = 2;

    //Paxos State
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
        handlesRequest(RequestId.SetValueRequest, this::handleSetValueRequest, SetValueRequest.class)
                .respondsWith(RequestId.SetValueResponse, SetValueResponse.class);

        handlesRequest(RequestId.GetValueRequest, this::handleGetValueRequest, GetValueRequest.class)
                .respondsWith(RequestId.GetValueRequest, GetValueResponse.class);

        //peer to peer message passing
        handlesMessage(RequestId.Prepare, this::prepare, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, PrepareResponse.class);

        handlesMessage(RequestId.ProposeRequest, this::handlePaxosProposal, ProposalRequest.class)
                .respondsWithMessage(RequestId.ProposeResponse, ProposalResponse.class);

        handlesMessage(RequestId.Commit, this::handlePaxosCommit, CommitRequest.class)
                .respondsWithMessage(RequestId.CommitResponse, CommitResponse.class);
    }

    private SetValueResponse handleSetValueRequest(SetValueRequest setValueRequest) {
        Optional<String> value = doPaxos(setValueRequest.getValue());
        return new SetValueResponse(value.orElse(""));
    }

    private GetValueResponse handleGetValueRequest(GetValueRequest getValueRequest) {
        Optional<String> value = doPaxos(null); //null is the default value
        return new GetValueResponse(value);
    }

    private CommitResponse handlePaxosCommit(CommitRequest req) {
        if (req.getId().isAfter(promisedGeneration)) {
            this.acceptedValue = Optional.of(req.getValue());
            return new CommitResponse(true);
        }
        return new CommitResponse(false);
    }

    private Optional<String> doPaxos(String value) {
        int attempts = 0;
        while (attempts <= maxAttempts) {
            attempts++;
            //Alice id=2 //fail
            //Bob id=4
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            PaxosResult paxosResult = doPaxos(monotonicId, value);
            if (paxosResult.success) {
                return paxosResult.value;
            } //else {
            //maxKnownPaxosRoundId = paxosResult.maxKnownNumber;
            //}
            Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), MILLISECONDS);
            logger.warn("Experienced Paxos contention. Attempting with higher generation");
        }
        throw new WriteTimeoutException(attempts);
    }

    static class PaxosResult {
        Optional<String> value;
        boolean success;

        public PaxosResult(Optional<String> value, boolean success) {
            this.value = value;
            this.success = success;
        }
    }

    private PaxosResult doPaxos(MonotonicId monotonicId, String value) {
        PrepareCallback prepareCallback = sendPrepareRequest(value, monotonicId);
        if (prepareCallback.isQuorumPrepared()) {
            String proposedValue = prepareCallback.getProposedValue();
            ProposalCallback proposalCallback = sendProposeRequest(proposedValue, monotonicId);
            if (proposalCallback.isQuorumAccepted()) {
                sendCommitRequest(monotonicId, proposedValue);
                return new PaxosResult(Optional.ofNullable(proposedValue), true);
            }
        }
        return new PaxosResult(Optional.empty(), false);
    }

    private BlockingQuorumCallback sendCommitRequest(MonotonicId monotonicId, String value) {
        BlockingQuorumCallback commitCallback = new BlockingQuorumCallback<>(getNoOfReplicas());
        sendMessageToReplicas(commitCallback, RequestId.Commit, new CommitRequest(monotonicId, value));
        return commitCallback;
    }

    private ProposalCallback sendProposeRequest(String proposedValue, MonotonicId monotonicId) {
        ProposalCallback proposalCallback = new ProposalCallback(getNoOfReplicas());
        sendMessageToReplicas(proposalCallback, RequestId.ProposeRequest, new ProposalRequest(monotonicId, proposedValue));
        return proposalCallback;
    }

    public static class PrepareCallback extends BlockingQuorumCallback<PrepareResponse> {
        private String proposedValue;

        public PrepareCallback(String proposedValue, int clusterSize) {
            super(clusterSize);
            this.proposedValue = proposedValue;
        }

        public String getProposedValue() {
            return getProposalValue(proposedValue, responses.values().stream().toList()); //TODO::
        }

        private String getProposalValue(String initialValue, List<PrepareResponse> promises) {
            PrepareResponse mostRecentAcceptedValue = getMostRecentAcceptedValue(promises);
            String proposedValue
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
                    .filter(p -> p.isPromised()).count() >= quorum;
        }
    }

    private PrepareCallback sendPrepareRequest(String proposedValue, MonotonicId monotonicId) {
        PrepareCallback prepareCallback = new PrepareCallback(proposedValue, getNoOfReplicas());
        sendMessageToReplicas(prepareCallback, RequestId.Prepare, new PrepareRequest(monotonicId));
        return prepareCallback;
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
