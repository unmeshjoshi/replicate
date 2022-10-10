package replicator.quorum;

import replicator.common.RequestId;
import replicator.common.RequestOrResponse;
import replicator.net.ClientConnection;
import replicator.net.InetAddressAndPort;
import replicator.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

abstract class QuorumCallback implements RequestCallback<RequestOrResponse> {
    final int quorum;
    private volatile int expectedNumberOfResponses;
    private volatile int receivedResponses;
    private volatile int receivedErrors;
    private volatile boolean done;
    private final ClientConnection clientConnection;
    List<String> successMessages = new ArrayList<>();
    List<String> errorMessages = new ArrayList<>();
    private Integer correlationId;

    public QuorumCallback(int totalExpectedResponses, ClientConnection clientConnection, Integer correlationId) {
        this.expectedNumberOfResponses = totalExpectedResponses;
        this.quorum = expectedNumberOfResponses / 2 + 1;
        this.correlationId = correlationId;
        this.clientConnection = clientConnection;
    }


    @Override
    public void onResponse(RequestOrResponse response, InetAddressAndPort address) {
        receivedResponses++;
        addResponseMessage(response);
        //TODO: Assignment 3. Complete client response handler.
        if (receivedResponses == quorum && !done) {
            CompletableFuture<String> clientResponse = processQuorumResponses();
            clientResponse.whenComplete((r, e) -> {
                respondToClient(r);
                done = true;
            });
        }
    }

    private void addResponseMessage(RequestOrResponse response) {
        String responseMessage = new String(response.getMessageBodyJson());
        if ("Success".equals(responseMessage)) {
            successMessages.add(responseMessage);
        } else {
            errorMessages.add(responseMessage);
        }
    }

    @Override
    public void onError(Exception t) {
        receivedErrors++;
        if (receivedErrors == quorum && !done) {
            respondToClient("Error");
            done = true;
        }
    }

    private void respondToClient(String response) {
        clientConnection.write(new RequestOrResponse(RequestId.SetValueResponse.getId(), response.getBytes(), correlationId));
    }

    abstract CompletableFuture<String> processQuorumResponses();
}
