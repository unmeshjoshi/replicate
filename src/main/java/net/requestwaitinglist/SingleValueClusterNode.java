package net.requestwaitinglist;

import net.pipeline.PipelinedConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.Config;
import common.JsonSerDes;
import common.Message;
import common.RequestOrResponse;
import requests.SetValueRequest;
import common.RequestId;
import net.ClientConnection;
import net.InetAddressAndPort;
import net.SocketClient;
import net.SocketListener;
import net.nioserver.zkstyle.NIOSocketListener;
import common.SystemClock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SingleValueClusterNode {
    private static Logger logger = LogManager.getLogger(SingleValueClusterNode.class);
    private final NIOSocketListener clientListener;
    private SystemClock clock;
    private Config config;
    private InetAddressAndPort listenAddress;
    private List<InetAddressAndPort> peers;
    private SocketListener listener;

    static class KV {
        String key;
        String value;
        long timestamp;

        public KV(String key, String value, long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }

        //for jackson
        private KV() {
        }
    }

    KV kv;

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

    public SingleValueClusterNode(SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort listenAddress, List<InetAddressAndPort> peers) throws IOException {
        this.clock = clock;
        this.config = config;
        this.listenAddress = listenAddress;
        this.peers = peers;
        this.listener = new SocketListener(this::handleServerMessage, listenAddress, config);
        this.clientListener = new NIOSocketListener(message -> {
            RequestOrResponse request = message.getRequest();
            if (request.getRequestId() == RequestId.SetValueRequest.getId()) {
                handleClientRequestRequiringQuorum(peers, request, new WriteQuorumCallback(peers.size(), request, message.getClientConnection()), RequestId.SetValueRequest);

            } else if (request.getRequestId() == RequestId.GetValueRequest.getId()) {
                handleClientRequestRequiringQuorum(peers, request, new ReadQuorumCallback(peers.size(), request, message.getClientConnection()), RequestId.GetValueRequest);
            }
        }, clientAddress);


        requestWaitingList = new RequestWaitingList(clock);
    }

    //<codeFragment name="registry">
    RequestWaitingList requestWaitingList;
    //</codeFragment>

    //<codeFragment name="handleSetValueClientRequest">
    int requestNumber;

    private void forwardClientRequestTo(InetAddressAndPort receiver, RequestOrResponse request, ClientConnection clientConnection) {
        RequestCallback callback = createResponseCallback(request, clientConnection);
        int requestId = nextRequestId();
        requestWaitingList.add(requestId, callback);
        try {
            SocketClient client = new SocketClient(receiver);
            client.sendOneway(new RequestOrResponse(RequestId.SetValueRequest.getId(), request.getMessageBodyJson(), requestId, listenAddress));
        } catch (Exception e) {
            callback.onError(e);
        }
    }

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

    //<codeFragment name="handleSetValueClientRequestRequiringQuorum">
    private void handleClientRequestRequiringQuorum(List<InetAddressAndPort> replicas, RequestOrResponse request, RequestCallback requestCallback, RequestId requestId) {
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, requestCallback);
            try {
                SocketClient client = new SocketClient(replica);
                client.sendOneway(new RequestOrResponse(requestId.getId(), request.getMessageBodyJson(), correlationId, listenAddress));
            } catch (IOException e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
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
        }
    }

    private void handleGetValueRequest(RequestOrResponse request) {
        sendResponseMessage(new RequestOrResponse(request.getGroupId(), RequestId.GetValueResponse.getId(), JsonSerDes.serialize(kv), request.getCorrelationId(), listenAddress), request.getFromAddress());
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
        kv = new KV(setValueRequest.getKey(), setValueRequest.getValue(), clock.now());
        sendResponseMessage(new RequestOrResponse(request.getGroupId(), RequestId.SetValueResponse.getId(), "Success".getBytes(), request.getCorrelationId(), listenAddress), request.getFromAddress());
    }

    private void sendResponseMessage(RequestOrResponse message, InetAddressAndPort fromAddress) {
        try {
            SocketClient socketClient = new SocketClient(fromAddress);
            socketClient.sendOneway(message);
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + fromAddress);
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
        List<KV> responses = new ArrayList<>();

        public ReadQuorumCallback(int totalExpectedResponses, RequestOrResponse clientRequest, ClientConnection clientConnection) {
            this.expectedNumberOfResponses = totalExpectedResponses;
            this.quorum = expectedNumberOfResponses / 2 + 1;
            this.request = clientRequest;
            this.clientConnection = clientConnection;
        }

        @Override
        public void onResponse(RequestOrResponse response) {
            receivedResponses++;
            KV kvResponse = JsonSerDes.deserialize(response.getMessageBodyJson(), KV.class);
            responses.add(kvResponse);
            if (receivedResponses == quorum && !done) {
                respondToClient(responses.get(0).value);
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
