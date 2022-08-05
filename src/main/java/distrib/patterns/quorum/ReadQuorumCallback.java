package distrib.patterns.quorum;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.RequestId;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.requests.SetValueRequest;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

class ReadQuorumCallback implements RequestCallback<RequestOrResponse> {
    private final int quorum;
    private volatile int expectedNumberOfResponses;
    private volatile int receivedResponses;
    private volatile int receivedErrors;
    private volatile boolean done;

    private final RequestOrResponse request;
    private final ClientConnection clientConnection;
    Map<InetAddressAndPort, StoredValue> responses = new HashMap<>();
    private int correlationId;

    public ReadQuorumCallback(int totalExpectedResponses, RequestOrResponse clientRequest, ClientConnection clientConnection) {
        this.expectedNumberOfResponses = totalExpectedResponses;
        this.quorum = expectedNumberOfResponses / 2 + 1;
        this.request = clientRequest;
        this.clientConnection = clientConnection;
    }

    @Override
    public void onResponse(RequestOrResponse response) {
        receivedResponses++;
        try {
            StoredValue kvResponse = JsonSerDes.deserialize(response.getMessageBodyJson(), StoredValue.class);
            responses.put(response.getFromAddress(), kvResponse);
            if (receivedResponses == quorum && !done) {
                respondToClient(pickLatestValue()); // homework do possible read-repair.
                //TODO:Assignment 4 call readRepair.
                //readRepair();
                done = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void readRepair() {
        StoredValue latestStoredValue = getLatestStoredValue();
        List<InetAddressAndPort> nodesHavingStaleValues = getNodesHavingStaleValues(latestStoredValue.getTimestamp());
        for (InetAddressAndPort nodesHavingStaleValue : nodesHavingStaleValues) {
            try {
                SocketClient client = new SocketClient(nodesHavingStaleValue);
                client.sendOneway(createSetValueRequest(latestStoredValue.getKey(), latestStoredValue.getValue()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private RequestOrResponse createSetValueRequest(String key, String value) {
        SetValueRequest setValueRequest = new SetValueRequest(key, value);
        RequestOrResponse requestOrResponse = new RequestOrResponse(RequestId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), correlationId++);
        return requestOrResponse;
    }

    private List<InetAddressAndPort> getNodesHavingStaleValues(long latestTimestamp) {
        return responses.entrySet().stream().filter(e -> e.getValue().getTimestamp() < latestTimestamp).map(e -> e.getKey()).collect(Collectors.toList());
    }

    private String pickLatestValue() {
        return getLatestStoredValue().getValue();
    }

    private StoredValue getLatestStoredValue() {
        return this.responses.values().stream().max(Comparator.comparingLong(StoredValue::getTimestamp)).orElse(StoredValue.EMPTY);
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
