package distrib.patterns.quorum;

import distrib.patterns.net.ClientConnection;

import java.util.concurrent.CompletableFuture;

class WriteQuorumCallback extends QuorumCallback {

    public WriteQuorumCallback(int totalExpectedResponses, ClientConnection clientConnection, Integer correlationId) {
        super(totalExpectedResponses, clientConnection, correlationId);
    }

    CompletableFuture<String> processQuorumResponses() {
        //some random logic to return error if quorum returns error messages.
        return CompletableFuture.completedFuture(successMessages.size() >= quorum ? successMessages.get(0) : errorMessages.get(0));
    }

}
