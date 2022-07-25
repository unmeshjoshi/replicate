package net.requestwaitinglist;

import common.*;
import net.ClientConnection;
import net.InetAddressAndPort;
import net.SocketClient;
import net.SocketListener;
import net.nioserver.zkstyle.NIOSocketListener;
import net.pipeline.PipelinedConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import paxos.*;
import requests.SetValueRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SingleValuePaxosClusterNode {
    private static Logger logger = LogManager.getLogger(SingleValuePaxosClusterNode.class);
    private final NIOSocketListener clientListener;
    private SystemClock clock;
    private Config config;
    private InetAddressAndPort listenAddress;
    private List<InetAddressAndPort> peers;
    private SocketListener listener;

    MonotonicId promisedGeneration = MonotonicId.empty();

    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<String> acceptedValue = Optional.empty();

    Optional<String> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();

    static class Network {
        List<InetAddressAndPort> connectionsToDrop = new ArrayList<>();

        public void dropMessagesTo(InetAddressAndPort address) {
            connectionsToDrop.add(address);
        }

        public void sendOneway(InetAddressAndPort address, RequestOrResponse request) {
            if (connectionsToDrop.contains(address)) {
                return; //drop the message
            }

        }
    }

    public SingleValuePaxosClusterNode(SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort listenAddress, List<InetAddressAndPort> peers) throws IOException {
        this.clock = clock;
        this.config = config;
        this.listenAddress = listenAddress;
        this.peers = peers;
        this.listener = new SocketListener(this::handleServerMessage, listenAddress, config);
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
                doPaxos(peers, request, waitingList, RequestId.SetValueRequest);

            } else if (request.getRequestId() == RequestId.GetValueRequest.getId()) {
                doPaxos(peers, request, waitingList, RequestId.GetValueRequest);
            }
        }, clientAddress);


        requestWaitingList = new RequestWaitingList(clock);
    }

    //<codeFragment name="registry">
    RequestWaitingList requestWaitingList;
    //</codeFragment>

    //<codeFragment name="handleSetValueClientRequest">
    int requestNumber;

    private int nextRequestId() {
        return requestNumber++;
    }

    private RequestCallback createResponseCallback(RequestOrResponse request, ClientConnection clientConnection) {
        return new RequestCallback<RequestOrResponse>() {
            @Override
            public void onResponse(RequestOrResponse r) {
                RequestOrResponse response = new RequestOrResponse(RequestId.SetValueResponse.getId(), r.getMessageBodyJson(), request.getCorrelationId());
                clientConnection.write(response);
            }

            @Override
            public void onError(Throwable e) {
                RequestOrResponse response = new RequestOrResponse(RequestId.SetValueResponse.getId(), e.getMessage().getBytes(), request.getCorrelationId());
                clientConnection.write(response);
            }
        };

    }

    //</codeFragment>
    int maxKnownPaxosRoundId = 1;
    int serverId = 1;


    int maxAttempts = 2;

    //<codeFragment name="handleSetValueClientRequestRequiringQuorum">
    private void doPaxos(List<InetAddressAndPort> replicas, RequestOrResponse request, RequestWaitingList waitingList, RequestId requestId) {
        SetValueRequest setValueRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), SetValueRequest.class);
        int attempts = 0;
        while (attempts <= maxAttempts) {
            attempts++;
            MonotonicId monotonicId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            Optional<String> proposedValue = doPaxos(monotonicId, setValueRequest.getValue(), replicas);
            if (proposedValue.isPresent() && proposedValue.get().equals(setValueRequest.getValue())) {
                waitingList.handleResponse(request.getCorrelationId(), proposedValue.get());
                return;
            }

            sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), MILLISECONDS);
            logger.warn("Experienced Paxos contention. Attempting with higher generation");

        }
        throw new WriteTimeoutException(attempts);
    }

    private void sleepUninterruptibly(int nextInt, TimeUnit milliseconds) {
        try {
            Thread.sleep(1000); //FIXME add uninterruptables dependecy?
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Optional<String> doPaxos(MonotonicId monotonicId, String value, List<InetAddressAndPort> replicas) {
        PrepareCallback prepareCallback = sendPrepareRequest(value, monotonicId, replicas);
        if (prepareCallback.isQuorumPrepared()) {
            String proposedValue = prepareCallback.getProposedValue();
            ProposalCallback proposalCallback = sendProposeRequest(proposedValue, monotonicId, replicas);
            if (proposalCallback.isQuorumAccepted()) {
                sendCommitRequest(monotonicId, replicas);
                return Optional.of(proposedValue);
            }
        }
        return Optional.empty();
    }

    private CommitCallback sendCommitRequest(MonotonicId monotonicId, List<InetAddressAndPort> replicas) {
        CommitCallback commitCallback = new CommitCallback(monotonicId);
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, commitCallback);
            try {
                SocketClient client = new SocketClient(replica);
                RequestOrResponse message = new RequestOrResponse(RequestId.PrepareRequest.getId(), JsonSerDes.serialize(new PrepareRequest(monotonicId)), correlationId, listenAddress);
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
                RequestOrResponse message = new RequestOrResponse(RequestId.ProposeRequest.getId(), JsonSerDes.serialize(proposalRequest), correlationId, listenAddress);
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
                //TODO
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
                        JsonSerDes.serialize(new PrepareRequest(monotonicId)), correlationId, listenAddress);
                sendMessage(message, replica);

            } catch (Exception e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
        return prepareCallback;
    }
    //</codeFragment>

    //<codeFragment name="handleSetValueClientRequestWithRequestPipeline">
    private void handleSetValueClientRequestWithRequestPipeline(InetAddressAndPort receiver, ClientConnection clientConnection, RequestOrResponse request) {
        RequestCallback future = createResponseCallback(request, clientConnection);
        int requestId = nextRequestId();
        requestWaitingList.add(requestId, future);
        PipelinedConnection connection = getConnectionTo(receiver);
        connection.send(new RequestOrResponse(RequestId.SetValueRequest.getId(), request.getMessageBodyJson(), requestNumber, listenAddress));
    }

    private PipelinedConnection getConnectionTo(InetAddressAndPort receiver) {
        PipelinedConnection client = new PipelinedConnection(receiver, 1000, response -> {
            handleServerMessage(new Message(response, RequestId.valueOf(response.getRequestId())));
        });
        client.start();
        return client;
    }
    //</codeFragment>

    public void start() {
        listener.start();
        clientListener.start();
    }

    //<codeFragment name="handleServerMessage">
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
            this.acceptedValue = Optional.of(request.getProposedValue());
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
        sendMessage(new RequestOrResponse(request.getGroupId(), RequestId.GetValueResponse.getId(), JsonSerDes.serialize(acceptedValue), request.getCorrelationId(), listenAddress), request.getFromAddress());
    }
    //</codeFragment>

    //<codeFragment name="handleSetValueResponse">
    private void handleResponse(RequestOrResponse response) {
        requestWaitingList.handleResponse(response.getCorrelationId(), response);
    }
    //</codeFragment>

    //<codeFragment name="handleSetValueRequest">
    private void handleSetValueRequest(RequestOrResponse request) {
        SetValueRequest setValueRequest = deserialize(request);
        sendMessage(new RequestOrResponse(request.getGroupId(), RequestId.SetValueResponse.getId(), setValueRequest.getValue().getBytes(), request.getCorrelationId(), listenAddress), request.getFromAddress());
    }

    private void sendMessage(RequestOrResponse message, InetAddressAndPort toAddress) {
        try {
            if (toAddress.equals(listenAddress)) {
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
    //</codeFragment>

    private SetValueRequest deserialize(RequestOrResponse request) {
        return JsonSerDes.deserialize(request.getMessageBodyJson(), SetValueRequest.class);
    }

    static class ReadQuorumCallback implements RequestCallback<RequestOrResponse> {
        private final int quorum;
        private volatile int expectedNumberOfResponses;
        private volatile int receivedResponses;
        private volatile int receivedErrors;
        private volatile boolean done;

        private final RequestOrResponse request;
        private final ClientConnection clientConnection;
        List<String> responses = new ArrayList<>();

        public ReadQuorumCallback(int totalExpectedResponses, RequestOrResponse clientRequest, ClientConnection clientConnection) {
            this.expectedNumberOfResponses = totalExpectedResponses;
            this.quorum = expectedNumberOfResponses / 2 + 1;
            this.request = clientRequest;
            this.clientConnection = clientConnection;
        }

        @Override
        public void onResponse(RequestOrResponse response) {
            receivedResponses++;
            String kvResponse = new String(response.getMessageBodyJson());
            responses.add(kvResponse);
            if (receivedResponses == quorum && !done) {
                respondToClient(responses.get(0));
                done = true;
            }
        }

        @Override
        public void onError(Throwable t) {
            receivedErrors++;
            if (receivedErrors == quorum && !done) {
                respondToClient("Error");
                done = true;
            }
        }


        private void respondToClient(String response) {
            clientConnection.write(new RequestOrResponse(RequestId.SetValueResponse.getId(), response.getBytes(), request.getCorrelationId()));
        }
    }


    //<codeFragment name="QuorumResponseHandler">
    static class WriteQuorumCallback implements RequestCallback<RequestOrResponse> {
        private final int quorum;
        private volatile int expectedNumberOfResponses;
        private volatile int receivedResponses;
        private volatile int receivedErrors;
        private volatile boolean done;

        private final RequestOrResponse request;
        private final ClientConnection clientConnection;

        public WriteQuorumCallback(int totalExpectedResponses, RequestOrResponse clientRequest, ClientConnection clientConnection) {
            this.expectedNumberOfResponses = totalExpectedResponses;
            this.quorum = expectedNumberOfResponses / 2 + 1;
            this.request = clientRequest;
            this.clientConnection = clientConnection;
        }

        @Override
        public void onResponse(RequestOrResponse response) {
            receivedResponses++;
            if (receivedResponses == quorum && !done) {
                respondToClient("Success");
                done = true;
            }
        }

        @Override
        public void onError(Throwable t) {
            receivedErrors++;
            if (receivedErrors == quorum && !done) {
                respondToClient("Error");
                done = true;
            }
        }


        private void respondToClient(String response) {
            clientConnection.write(new RequestOrResponse(RequestId.SetValueResponse.getId(), response.getBytes(), request.getCorrelationId()));
        }
    }
//</codeFragment>
}
