package distrib.patterns.quorum;

import distrib.patterns.common.*;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;

import java.io.IOException;

class ClientRequestHandler {
    private final NIOSocketListener clientListener;
    private final InetAddressAndPort clientConnectionAddress;
    private final ClientState clientState;
    private QuorumKVStore kvStore;
    private int noOfReplicas;
    private boolean doSyncReadRepair;

    public ClientRequestHandler(InetAddressAndPort clientConnectionAddress, SystemClock clock, QuorumKVStore kvStore, boolean doSyncReadRepair) throws IOException {
        this.clientConnectionAddress = clientConnectionAddress;
        this.clientState = new ClientState(clock);
        this.clientListener = new NIOSocketListener(message -> {
            handleClientRequest(message);
        }, clientConnectionAddress);
        this.kvStore = kvStore;
        this.noOfReplicas = kvStore.getNoOfReplicas();
        this.doSyncReadRepair = doSyncReadRepair;
    }

    private void handleClientRequest(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        if (request.getRequestId() == RequestId.SetValueRequest.getId()) {
            byte[] messageBodyJson = request.getMessageBodyJson();
            SetValueRequest clientSetValueRequest = JsonSerDes.deserialize(messageBodyJson, SetValueRequest.class);
            handleSetValueRequest(message.getClientConnection(), request.getCorrelationId(), clientSetValueRequest);

        } else if (request.getRequestId() == RequestId.GetValueRequest.getId()) {
            byte[] messageBodyJson = request.getMessageBodyJson();
            GetValueRequest clientGetValueRequest = JsonSerDes.deserialize(messageBodyJson, GetValueRequest.class);
            handleGetValueRequest(message.getClientConnection(), request.getCorrelationId(), request.getGeneration(), RequestId.GetValueRequest, clientGetValueRequest);
        }
    }


    private void handleSetValueRequest(ClientConnection clientConnection, Integer correlationId, SetValueRequest clientSetValueRequest) {
        SetValueRequest requestToReplicas = new SetValueRequest(clientSetValueRequest.getKey(),
                clientSetValueRequest.getValue(),
                clientSetValueRequest.getClientId(),
                clientSetValueRequest.getRequestNumber(),
                clientState.getTimestamp()); //assign timestamp to request.
        WriteQuorumCallback quorumCallback = new WriteQuorumCallback(kvStore.getNoOfReplicas(), clientConnection, correlationId);
        kvStore.sendRequestToReplicas(quorumCallback, RequestId.SetValueRequest, requestToReplicas);
    }


    private void handleGetValueRequest(ClientConnection clientConnection, Integer correlationId, int generation, RequestId requestId, GetValueRequest clientSetValueRequest) {
        GetValueRequest request = new GetValueRequest(clientSetValueRequest.getKey());
        ReadQuorumCallback quorumCallback = new ReadQuorumCallback(kvStore, noOfReplicas, clientConnection, correlationId, generation, doSyncReadRepair);
        kvStore.sendRequestToReplicas(quorumCallback, requestId, request);
    }

    public void start() {
        clientListener.start();
    }

    public InetAddressAndPort getClientConnectionAddress() {
        return clientConnectionAddress;
    }
}
