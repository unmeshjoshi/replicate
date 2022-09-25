package distrib.patterns.paxoskv;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.net.SocketClient;
import distrib.patterns.net.SocketListener;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.common.MonotonicId;
import distrib.patterns.paxos.WriteTimeoutException;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class PaxosState {
    MonotonicId promisedGeneration = MonotonicId.empty();
    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<String> acceptedValue = Optional.empty();

    Optional<String> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();

}

public class PaxosKVClusterNode {
    private static Logger logger = LogManager.getLogger(PaxosKVClusterNode.class);
    private final NIOSocketListener clientListener;
    private SystemClock clock;
    private Config config;
    private InetAddressAndPort peerConnectionAddress;
    private List<InetAddressAndPort> peers;
    private SocketListener listener;

    //Paxos State
    Map<String, PaxosState> kv = new HashMap<>();
    public PaxosKVClusterNode(SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        this.clock = clock;
        this.config = config;
        this.peerConnectionAddress = peerConnectionAddress;
        this.peers = peers;
        this.listener = new SocketListener(this::handleServerMessage, peerConnectionAddress, config);
        RequestWaitingList waitingList = new RequestWaitingList(clock);
        this.clientListener = new NIOSocketListener(message -> {
            RequestOrResponse request = message.getRequest();
            if (request.getRequestId() == RequestId.SetValueRequest.getId()) {
                var callback = new RequestCallback() {
                    @Override
                    public void onResponse(Object r, InetAddressAndPort address) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(r), request.getCorrelationId()));
                    }

                    @Override
                    public void onError(Exception e) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(e.getMessage()), request.getCorrelationId()));
                    }
                };
                waitingList.add(request.getCorrelationId(), callback);
                SetValueRequest setValueRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), SetValueRequest.class);
                doPaxos(peers, request, waitingList, setValueRequest.getKey(), setValueRequest.getValue());

            } else if (request.getRequestId() == RequestId.GetValueRequest.getId()) {
                var callback = new RequestCallback() {
                    @Override
                    public void onResponse(Object r, InetAddressAndPort address) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(r), request.getCorrelationId()));
                    }

                    @Override
                    public void onError(Exception e) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(e.getMessage()), request.getCorrelationId()));
                    }
                };
                waitingList.add(request.getCorrelationId(), callback);
                GetValueRequest getValueRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), GetValueRequest.class);
                doPaxos(peers, request, waitingList, getValueRequest.getKey(), null);
            }
        }, clientAddress);

        requestWaitingList = new RequestWaitingList(clock);
    }

    RequestWaitingList requestWaitingList;
    int requestNumber;

    private int nextRequestId() {
        return requestNumber++;
    }

    int maxKnownPaxosRoundId = 1;
    int serverId = 1;
    int maxAttempts = 2;

    private void doPaxos(List<InetAddressAndPort> replicas, RequestOrResponse request, RequestWaitingList waitingList, String key, String value) {
        int attempts = 0;
        while (attempts <= maxAttempts) {
            attempts++;
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            PaxosResult paxosResult = doPaxos(monotonicId, key, value, replicas);
            if (paxosResult.success) {
                waitingList.handleResponse(request.getCorrelationId(), paxosResult.value);
                return;
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

    private PaxosResult doPaxos(MonotonicId monotonicId, String key, String value, List<InetAddressAndPort> replicas) {
        PrepareCallback prepareCallback = sendPrepareRequest(key, value, monotonicId, replicas);
        if (prepareCallback.isQuorumPrepared()) {
            String proposedValue = prepareCallback.getProposedValue();
            ProposalCallback proposalCallback = sendProposeRequest(key, proposedValue, monotonicId, replicas);
            if (proposalCallback.isQuorumAccepted()) {
                sendCommitRequest(key, proposedValue, monotonicId, replicas);
                return new PaxosResult(Optional.ofNullable(proposedValue), true);
            }
        }
        return new PaxosResult(Optional.empty(), false);
    }

    private CommitCallback sendCommitRequest(String key, String value, MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        CommitCallback commitCallback = new CommitCallback(monotonicId);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, commitCallback);
            try {
                SocketClient client = new SocketClient(replica);
                RequestOrResponse message = new RequestOrResponse(RequestId.Commit.getId(), JsonSerDes.serialize(new CommitRequest(key, value, monotonicId)), correlationId, peerConnectionAddress);
                client.sendOneway(message);
            } catch (IOException e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
        return commitCallback;
    }

    private ProposalCallback sendProposeRequest(String key, String proposedValue, MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        ProposalCallback proposalCallback = new ProposalCallback(proposedValue);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, proposalCallback);
            try {
                ProposalRequest proposalRequest = new ProposalRequest(monotonicId, key, proposedValue);
                RequestOrResponse message = new RequestOrResponse(RequestId.ProposeRequest.getId(), JsonSerDes.serialize(proposalRequest), correlationId, peerConnectionAddress);
                sendMessage(message, replica);
            } catch (Exception e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
        return proposalCallback;
    }

    static class PrepareCallback implements RequestCallback<RequestOrResponse> {
        CountDownLatch latch = new CountDownLatch(3);
        List<PrepareResponse> promises = new ArrayList<>();
        private String proposedValue;

        public PrepareCallback(String proposedValue) {
            this.proposedValue = proposedValue;
        }

        @Override
        public void onResponse(RequestOrResponse r, InetAddressAndPort address) {
            promises.add(JsonSerDes.deserialize(r.getMessageBodyJson(), PrepareResponse.class));
            latch.countDown();
        }

        @Override
        public void onError(Exception e) {
        }

        public String getProposedValue() {
            return getProposalValue(proposedValue, promises);
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
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        }
    }


    private PrepareCallback sendPrepareRequest(String key, String proposedValue, MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        PrepareCallback prepareCallback = new PrepareCallback(proposedValue);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, prepareCallback);
            try {
                RequestOrResponse message = new RequestOrResponse(RequestId.PrepareRequest.getId(),
                        JsonSerDes.serialize(new PrepareRequest(key, monotonicId)), correlationId, peerConnectionAddress);
                sendMessage(message, replica);

            } catch (Exception e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
        return prepareCallback;
    }

    public void start() {
        listener.start();
        clientListener.start();
    }

    private synchronized void handleServerMessage(Message<RequestOrResponse> message) {
        RequestOrResponse requestOrResponse = message.getRequest();
        if (requestOrResponse.getRequestId() == RequestId.PrepareRequest.getId()) {
            handlePaxosPrepare(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.Promise.getId()) {
            requestWaitingList.handleResponse(requestOrResponse.getCorrelationId(), requestOrResponse, requestOrResponse.getFromAddress());

        } else if (requestOrResponse.getRequestId() == RequestId.ProposeRequest.getId()) {
            handlePaxosProposal(requestOrResponse.getCorrelationId(), requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.ProposeResponse.getId()) {
            requestWaitingList.handleResponse(requestOrResponse.getCorrelationId(), requestOrResponse, requestOrResponse.getFromAddress());

        } else if (requestOrResponse.getRequestId() == RequestId.Commit.getId()) {
            handlePaxosCommit(requestOrResponse.getCorrelationId(), requestOrResponse);
        }
    }

    private void handlePaxosCommit(Integer correlationId, RequestOrResponse requestOrResponse) {
        CommitRequest request = JsonSerDes.deserialize(requestOrResponse.getMessageBodyJson(), CommitRequest.class);
        PaxosState paxosState = getOrCreatePaxosState(request.getKey());
        paxosState.committedGeneration = Optional.of(request.getMonotonicId());
        paxosState.committedValue = Optional.of(request.getValue());
        sendMessage(new RequestOrResponse(RequestId.ProposeResponse.getId(), JsonSerDes.serialize(true), correlationId), requestOrResponse.getFromAddress());
    }

    private void handlePaxosProposal(Integer correlationId, RequestOrResponse requestOrResponse) {
        ProposalRequest request = JsonSerDes.deserialize(requestOrResponse.getMessageBodyJson(), ProposalRequest.class);
        MonotonicId generation = request.getMonotonicId();
        PaxosState paxosState = getOrCreatePaxosState(request.getKey());
        if (generation.equals(paxosState.promisedGeneration) || generation.isAfter(paxosState.promisedGeneration)) {
            paxosState.promisedGeneration = generation;
            paxosState.acceptedGeneration = Optional.of(generation);
            paxosState.acceptedValue = Optional.ofNullable(request.getProposedValue());
            sendMessage(new RequestOrResponse(RequestId.ProposeResponse.getId(), JsonSerDes.serialize(true), correlationId), requestOrResponse.getFromAddress());
        }
    }

    private void handlePaxosPrepare(RequestOrResponse requestOrResponse) {
        PrepareRequest request = JsonSerDes.deserialize(requestOrResponse.getMessageBodyJson(), PrepareRequest.class);
        PrepareResponse prepareResponse = prepare(request.key, request.monotonicId);
        sendMessage(new RequestOrResponse(RequestId.Promise.getId(), JsonSerDes.serialize(prepareResponse), requestOrResponse.getCorrelationId()), requestOrResponse.getFromAddress());
    }

    public PrepareResponse prepare(String key, MonotonicId generation) {
        PaxosState paxosState = getOrCreatePaxosState(key);
        if (paxosState.promisedGeneration.isAfter(generation)) {
            return new PrepareResponse(false, paxosState.acceptedValue, paxosState.acceptedGeneration);
        }
        paxosState.promisedGeneration = generation;
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

    private void handleResponse(RequestOrResponse response) {
        requestWaitingList.handleResponse(response.getCorrelationId(), response);
    }

    private void handleSetValueRequest(RequestOrResponse request) {
        SetValueRequest setValueRequest = deserialize(request);
        sendMessage(new RequestOrResponse(request.getGeneration(), RequestId.SetValueResponse.getId(), setValueRequest.getValue().getBytes(), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
    }

//    static class Network {
//        List<InetAddressAndPort> dropMessagesTo = new ArrayList<>();
//        public void dropMessagesTo(InetAddressAndPort address) {
//            dropMessagesTo.add(address);
//        }
//        public void sendOneWay(InetAddressAndPort address, RequestOrResponse requestOrResponse) {
//            if (dropMessagesTo.contains(address)) {
//                return;
//            }
//            try {
//
//                SocketClient socketClient = new SocketClient(address);
//            socketClient.sendOneway(requestOrResponse);
//        }
//    }

    private void sendMessage(RequestOrResponse message, InetAddressAndPort toAddress) {
        try {
            if (toAddress.equals(peerConnectionAddress)) {
                handleServerMessage(new Message<RequestOrResponse>(message, RequestId.valueOf(message.getRequestId())));
                return;
            }
            SocketClient socketClient = new SocketClient(toAddress);
            socketClient.sendOneway(message);
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + toAddress);
            throw new RuntimeException(e);
        }
    }

    private SetValueRequest deserialize(RequestOrResponse request) {
        return JsonSerDes.deserialize(request.getMessageBodyJson(), SetValueRequest.class);
    }
}
