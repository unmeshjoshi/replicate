package distrib.patterns.quorumconsensus;

import distrib.patterns.common.RequestId;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

abstract class QuorumCallback implements RequestCallback<RequestOrResponse> {
    final int quorum;
    private final int expectedNumberOfResponses;
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
    public void onResponse(RequestOrResponse response) {
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
    public void onError(Throwable t) {
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
