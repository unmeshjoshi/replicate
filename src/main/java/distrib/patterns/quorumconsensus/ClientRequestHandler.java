package distrib.patterns.quorumconsensus;

import distrib.patterns.common.*;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;
import distrib.patterns.requests.VersionedSetValueRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
        GetVersionRequest getVersion = new GetVersionRequest(clientSetValueRequest.getKey());
        var expectedNumberOfResponses = 3;
        CompletableFuture<List<MonotonicId>> responseFuture = new CompletableFuture<>();

        RequestCallback versionCallback = new RequestCallback<RequestOrResponse>() {
            final int quorum  = expectedNumberOfResponses / 2 + 1;;
            private volatile int receivedResponses;
            private volatile int receivedErrors;
            private volatile boolean done;
            List<MonotonicId> versions = new ArrayList<>();

            @Override
            public void onResponse(RequestOrResponse response) {
                receivedResponses++;
                addResponseMessage(response);
                if (receivedResponses == quorum) {
                    responseFuture.complete(versions);
                }
            }

            private void addResponseMessage(RequestOrResponse response) {
                MonotonicId version = JsonSerDes.deserialize(response.getMessageBodyJson(), MonotonicId.class);
                versions.add(version);
            }

            @Override
            public void onError(Throwable e) {
                receivedErrors++;
                if (receivedErrors == quorum && !done) {
                    done = true;
                    responseFuture.completeExceptionally(new ReadTimeoutException("Timeout waiting for versions"));
                }
            }
        };
        kvStore.sendRequestToReplicas(versionCallback, RequestId.GetVersion, getVersion);

        responseFuture.whenComplete((r, e)->{
            if (e != null) {
                clientConnection.write(new RequestOrResponse(RequestId.SetValueResponse.getId(), JsonSerDes.serialize("Error"), correlationId));
                return;
            }
            assignVersionAndSetValue(clientSetValueRequest, clientConnection, correlationId, r);
        });

    }

    private void assignVersionAndSetValue(SetValueRequest clientSetValueRequest, ClientConnection clientConnection, Integer correlationId, List<MonotonicId> existingVersions) {
        VersionedSetValueRequest requestToReplicas = new VersionedSetValueRequest(clientSetValueRequest.getKey(),
                clientSetValueRequest.getValue(),
                clientSetValueRequest.getClientId(),
                clientSetValueRequest.getRequestNumber(),
                getNextId(existingVersions)
                ); //assign timestamp to request.
        WriteQuorumCallback quorumCallback = new WriteQuorumCallback(kvStore.getNoOfReplicas(), clientConnection, correlationId);
        kvStore.sendRequestToReplicas(quorumCallback, RequestId.SetValueRequest, requestToReplicas);
    }


    private MonotonicId getNextId(List<MonotonicId> ids) {
        MonotonicId max = getMax(ids);
        if (max.isEmpty()) {
            return new MonotonicId(1, 1);
        }

        int requestId = max.requestId + 1;
        return new MonotonicId(requestId, 1);
    }

    private MonotonicId getMax(List<MonotonicId> ids) {
        return ids.stream().max(MonotonicId::compareTo).orElse(MonotonicId.empty());
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
