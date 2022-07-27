package distrib.patterns.paxos;

import com.google.common.util.concurrent.Uninterruptibles;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.net.SocketClient;
import distrib.patterns.net.SocketListener;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.requests.SetValueRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SingleValuePaxosClusterNode {
    private static Logger logger = LogManager.getLogger(SingleValuePaxosClusterNode.class);
    private final NIOSocketListener clientListener;
    private SystemClock clock;
    private Config config;
    private InetAddressAndPort peerConnectionAddress;
    private List<InetAddressAndPort> peers;
    private SocketListener listener;

    //Paxos State
    MonotonicId promisedGeneration = MonotonicId.empty();

    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<String> acceptedValue = Optional.empty();

    Optional<String> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();

    public SingleValuePaxosClusterNode(SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
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
                    public void onResponse(Object r) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(r), request.getCorrelationId()));
                    }

                    @Override
                    public void onError(Throwable e) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(e.getMessage()), request.getCorrelationId()));
                    }
                };
                waitingList.add(request.getCorrelationId(), callback);
                SetValueRequest setValueRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), SetValueRequest.class);
                doPaxos(peers, request, waitingList, setValueRequest.getValue());

            } else if (request.getRequestId() == RequestId.GetValueRequest.getId()) {
                var callback = new RequestCallback() {
                    @Override
                    public void onResponse(Object r) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(r), request.getCorrelationId()));
                    }

                    @Override
                    public void onError(Throwable e) {
                        message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(e.getMessage()), request.getCorrelationId()));
                    }
                };
                waitingList.add(request.getCorrelationId(), callback);
                doPaxos(peers, request, waitingList, null);
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

    private void doPaxos(List<InetAddressAndPort> replicas, RequestOrResponse request, RequestWaitingList waitingList, String value) {
        int attempts = 0;
        while (attempts <= maxAttempts) {
            attempts++;
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            PaxosResult paxosResult = doPaxos(monotonicId, value, replicas);
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

    private PaxosResult doPaxos(MonotonicId monotonicId, String value, List<InetAddressAndPort> replicas) {
        PrepareCallback prepareCallback = sendPrepareRequest(value, monotonicId, replicas);
        if (prepareCallback.isQuorumPrepared()) {
            String proposedValue = prepareCallback.getProposedValue();
            ProposalCallback proposalCallback = sendProposeRequest(proposedValue, monotonicId, replicas);
            if (proposalCallback.isQuorumAccepted()) {
                sendCommitRequest(monotonicId, replicas);
                return new PaxosResult(Optional.ofNullable(proposedValue), true);
            }
        }
        return new PaxosResult(Optional.empty(), false);
    }

    private CommitCallback sendCommitRequest(MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        CommitCallback commitCallback = new CommitCallback(monotonicId);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, commitCallback);
            try {
                SocketClient client = new SocketClient(replica);
                RequestOrResponse message = new RequestOrResponse(RequestId.PrepareRequest.getId(), JsonSerDes.serialize(new PrepareRequest(monotonicId)), correlationId, peerConnectionAddress);
                client.sendOneway(message);
            } catch (IOException e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
        return commitCallback;
    }

    private ProposalCallback sendProposeRequest(String proposedValue, MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        ProposalCallback proposalCallback = new ProposalCallback(proposedValue);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, proposalCallback);
            try {
                ProposalRequest proposalRequest = new ProposalRequest(monotonicId, proposedValue);
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
        public void onResponse(RequestOrResponse r) {
            promises.add(JsonSerDes.deserialize(r.getMessageBodyJson(), PrepareResponse.class));
            latch.countDown();
        }

        @Override
        public void onError(Throwable e) {
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


    private PrepareCallback sendPrepareRequest(String proposedValue, MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        PrepareCallback prepareCallback = new PrepareCallback(proposedValue);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, prepareCallback);
            try {
                RequestOrResponse message = new RequestOrResponse(RequestId.PrepareRequest.getId(),
                        JsonSerDes.serialize(new PrepareRequest(monotonicId)), correlationId, peerConnectionAddress);
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
        if (requestOrResponse.getRequestId() == RequestId.SetValueRequest.getId()) {
            handleSetValueRequest(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.GetValueRequest.getId()) {
            handleGetValueRequest(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.SetValueResponse.getId()) {
            handleResponse(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.GetValueResponse.getId()) {
            handleResponse(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.PrepareRequest.getId()) {
            handlePaxosPrepare(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.Promise.getId()) {
            requestWaitingList.handleResponse(requestOrResponse.getCorrelationId(), requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.ProposeRequest.getId()) {
            handlePaxosProposal(requestOrResponse.getCorrelationId(), requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.ProposeResponse.getId()) {
            requestWaitingList.handleResponse(requestOrResponse.getCorrelationId(), requestOrResponse);
        }
    }

    private void handlePaxosProposal(Integer correlationId, RequestOrResponse requestOrResponse) {
        ProposalRequest request = JsonSerDes.deserialize(requestOrResponse.getMessageBodyJson(), ProposalRequest.class);
        MonotonicId generation = request.getMonotonicId();
        if (generation.equals(promisedGeneration) || generation.isAfter(promisedGeneration)) {
            this.promisedGeneration = generation;
            this.acceptedGeneration = Optional.of(generation);
            this.acceptedValue = Optional.ofNullable(request.getProposedValue());
            sendMessage(new RequestOrResponse(RequestId.ProposeResponse.getId(), JsonSerDes.serialize(true), correlationId), requestOrResponse.getFromAddress());
        }
    }

    private void handlePaxosPrepare(RequestOrResponse requestOrResponse) {
        PrepareRequest request = JsonSerDes.deserialize(requestOrResponse.getMessageBodyJson(), PrepareRequest.class);
        PrepareResponse prepareResponse = prepare(request.monotonicId);
        sendMessage(new RequestOrResponse(RequestId.Promise.getId(), JsonSerDes.serialize(prepareResponse), requestOrResponse.getCorrelationId()), requestOrResponse.getFromAddress());
    }

    public PrepareResponse prepare(MonotonicId generation) {
        if (promisedGeneration.isAfter(generation)) {
            return new PrepareResponse(false, acceptedValue, acceptedGeneration);
        }
        promisedGeneration = generation;
        return new PrepareResponse(true, acceptedValue, acceptedGeneration);
    }

    private void handleGetValueRequest(RequestOrResponse request) {
        sendMessage(new RequestOrResponse(request.getGeneration(), RequestId.GetValueResponse.getId(), JsonSerDes.serialize(acceptedValue), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
    }

    private void handleResponse(RequestOrResponse response) {
        requestWaitingList.handleResponse(response.getCorrelationId(), response);
    }

    private void handleSetValueRequest(RequestOrResponse request) {
        SetValueRequest setValueRequest = deserialize(request);
        sendMessage(new RequestOrResponse(request.getGeneration(), RequestId.SetValueResponse.getId(), setValueRequest.getValue().getBytes(), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
    }

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
