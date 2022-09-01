package distrib.patterns.quorum;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

class PeerMessagingService {
    private static Logger logger = LogManager.getLogger(QuorumKVStore.class);
    private final NIOSocketListener peerListener;
    private final InetAddressAndPort peerConnectionAddress;
    private final RequestWaitingList requestWaitingList;
    private final Network network = new Network();
    private final QuorumKVStore kvStore;


    public PeerMessagingService(InetAddressAndPort peerConnectionAddress, QuorumKVStore kvStore, SystemClock clock) throws IOException {
        this.peerListener = new NIOSocketListener(this::handleServerMessage, peerConnectionAddress);
        this.peerConnectionAddress = peerConnectionAddress;
        this.kvStore = kvStore;
        this.requestWaitingList = new RequestWaitingList(clock);
    }

    public void start() {
        peerListener.start();
    }

    public <T> void sendRequestToReplicas(RequestCallback requestCallback, RequestId requestId, T request) {
        for (InetAddressAndPort replica : kvStore.getReplicas()) {
            int correlationId = nextRequestId();
            sendRequestToReplica(requestCallback, replica, new RequestOrResponse(kvStore.getGeneration(), requestId.getId(), JsonSerDes.serialize(request), correlationId, peerConnectionAddress));
        }
    }

    public <T> void sendRequestToReplica(RequestCallback requestCallback, InetAddressAndPort replica, RequestOrResponse request) {
        requestWaitingList.add(request.getCorrelationId(), requestCallback);
        try {
            //Garbage Collection Pause 10 seconds
            network.sendOneWay(replica, request);
        } catch (IOException e) {
            requestWaitingList.handleError(request.getCorrelationId(), e);
        }
    }


    int requestNumber;
    private int nextRequestId() {
        return requestNumber++;
    }

    public void dropMessagesTo(QuorumKVStore clusterNode) {
        network.dropMessagesTo(clusterNode.getPeerConnectionAddress());
    }

    public void reconnectTo(QuorumKVStore clusterNode) {
        network.reconnectTo(clusterNode.getPeerConnectionAddress());
    }

    private <T> T deserialize(RequestOrResponse request, Class<T> clazz) {
        return JsonSerDes.deserialize(request.getMessageBodyJson(), clazz);
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
        }
    }

    private void handleGetValueRequest(RequestOrResponse request) {
        GetValueRequest getValueRequest = deserialize(request, GetValueRequest.class);
        sendResponseMessage(new RequestOrResponse(request.getGeneration(), RequestId.GetValueResponse.getId(), JsonSerDes.serialize(kvStore.get(getValueRequest.getKey())), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
    }

    private void handleResponse(RequestOrResponse response) {
        requestWaitingList.handleResponse(response.getCorrelationId(), response);
    }

    private void handleSetValueRequest(RequestOrResponse request) {
        int maxKnownGeneration = kvStore.maxKnownGeneration();
        Integer requestGeneration = request.getGeneration();
        if (requestGeneration < maxKnownGeneration) {
            String errorMessage = "Rejecting request from generation " + requestGeneration + " as already accepted from generation " + maxKnownGeneration;
            sendResponseMessage(new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(), errorMessage.getBytes(), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
            return;
        }

        //TODO: Assignment 3 Add check for generation while handling requests.
        SetValueRequest setValueRequest = deserialize(request, SetValueRequest.class);
        kvStore.put(setValueRequest.getKey(), new StoredValue(setValueRequest.getKey(), setValueRequest.getValue(), setValueRequest.getTimestamp(), requestGeneration));
        sendResponseMessage(new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(), "Success".getBytes(), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
    }

    private void sendResponseMessage(RequestOrResponse message, InetAddressAndPort fromAddress) {
        try {
            network.sendOneWay(fromAddress, message);
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + fromAddress);
        }
    }

    public InetAddressAndPort getPeerConnectionAddress() {
        return peerConnectionAddress;
    }

    public void dropMessagesAfter(QuorumKVStore byzantium, int dropAfterNoOfMessages) {
        network.dropMessagesAfter(byzantium.getPeerConnectionAddress(), dropAfterNoOfMessages);
    }
}
