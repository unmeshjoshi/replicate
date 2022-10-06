package distrib.patterns.paxoskv;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.paxos.*;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueRequest;
import distrib.patterns.quorum.messages.SetValueResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
        handlesRequest(RequestId.SetValueRequest, this::handleClientSetValueRequest, SetValueRequest.class);
        handlesRequest(RequestId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);

        //peer to peer message passing
        handlesMessage(RequestId.Prepare, this::prepare, PrepareRequest.class)
                .respondsWithMessage(RequestId.Promise, PrepareResponse.class);

        handlesMessage(RequestId.ProposeRequest, this::handlePaxosProposal, ProposalRequest.class)
                .respondsWithMessage(RequestId.ProposeResponse, ProposalResponse.class);

        handlesMessage(RequestId.Commit, this::handlePaxosCommit, CommitRequest.class)
                .respondsWithMessage(RequestId.CommitResponse, CommitResponse.class);
    }

    private GetValueResponse handleClientGetValueRequest(GetValueRequest request) {
        Optional<String> value = doPaxos(request.getKey(), null);
        return new GetValueResponse(value);
    }

    private SetValueResponse handleClientSetValueRequest(SetValueRequest setValueRequest) {
        Optional<String> value = doPaxos(setValueRequest.getKey(), setValueRequest.getValue());
        return new SetValueResponse(value.get());
    }

    int maxKnownPaxosRoundId = 1;
    int serverId = 1;
    int maxAttempts = 2;

    private Optional<String> doPaxos(String key, String value) {
        int attempts = 0;
        while (attempts <= maxAttempts) {
            attempts++;
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            PaxosResult paxosResult = doPaxos(monotonicId, key, value);
            if (paxosResult.success) {
                return paxosResult.value;
            }
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

    private PaxosResult doPaxos(MonotonicId monotonicId, String key, String value) {
        SingleValuePaxos.PrepareCallback prepareCallback = sendPrepareRequest(key, value, monotonicId);
        if (prepareCallback.isQuorumPrepared()) {
            String proposedValue = prepareCallback.getProposedValue();
            distrib.patterns.paxos.ProposalCallback proposalCallback = sendProposeRequest(key, proposedValue, monotonicId);
            if (proposalCallback.isQuorumAccepted()) {
                sendCommitRequest(key, proposedValue, monotonicId);
                return new PaxosResult(Optional.ofNullable(proposedValue), true);
            }
        }
        return new PaxosResult(Optional.empty(), false);
    }

    private BlockingQuorumCallback sendCommitRequest(String key, String value, MonotonicId monotonicId) {
        BlockingQuorumCallback commitCallback = new BlockingQuorumCallback<>(getNoOfReplicas());
        sendMessageToReplicas(commitCallback, RequestId.Commit, new CommitRequest(key, value, monotonicId));
        return commitCallback;
    }


    private distrib.patterns.paxos.ProposalCallback sendProposeRequest(String key, String proposedValue, MonotonicId monotonicId) {
        distrib.patterns.paxos.ProposalCallback proposalCallback = new distrib.patterns.paxos.ProposalCallback(getNoOfReplicas());
        sendMessageToReplicas(proposalCallback, RequestId.ProposeRequest, new ProposalRequest(monotonicId, key, proposedValue));
        return proposalCallback;
    }

    private SingleValuePaxos.PrepareCallback sendPrepareRequest(String key, String proposedValue, MonotonicId monotonicId) {
        SingleValuePaxos.PrepareCallback prepareCallback = new SingleValuePaxos.PrepareCallback(proposedValue, getNoOfReplicas());
        sendMessageToReplicas(prepareCallback, RequestId.Prepare, new PrepareRequest(key, monotonicId));
        return prepareCallback;
    }

    private CommitResponse handlePaxosCommit(CommitRequest request) {
        PaxosState paxosState = getOrCreatePaxosState(request.key);
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
