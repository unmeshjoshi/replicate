package distrib.patterns.quorum;

import distrib.patterns.common.RequestId;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.requestwaitinglist.RequestCallback;

import java.util.ArrayList;
import java.util.List;

class WriteQuorumCallback implements RequestCallback<RequestOrResponse> {
    private final int quorum;
    private volatile int expectedNumberOfResponses;
    private volatile int receivedResponses;
    private volatile int receivedErrors;
    private volatile boolean done;

    private final RequestOrResponse request;
    private final ClientConnection clientConnection;
    List<String> successMessages = new ArrayList<>();
    List<String> errorMessages = new ArrayList<>();

    public WriteQuorumCallback(int totalExpectedResponses, RequestOrResponse clientRequest, ClientConnection clientConnection) {
        this.expectedNumberOfResponses = totalExpectedResponses;
        this.quorum = expectedNumberOfResponses / 2 + 1;
        this.request = clientRequest;
        this.clientConnection = clientConnection;
    }

    @Override
    public void onResponse(RequestOrResponse response) {
        receivedResponses++;
        addResponseMessage(response);
        //TODO: Assignment 3. Complete client response handler.
//        if (receivedResponses == quorum && !done) {
//            respondToClient(prepareResponse());
//            done = true;
//        }
    }

    private void addResponseMessage(RequestOrResponse response) {
        String responseMessage = new String(response.getMessageBodyJson());
        if ("Success".equals(responseMessage)) {
            successMessages.add(responseMessage);
        } else {
            errorMessages.add(responseMessage);
        }
    }

    private String prepareResponse() {
        //some random logic to return error if quorum returns error messages.
        return successMessages.size() >= quorum ? successMessages.get(0) : errorMessages.get(0);
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
