package distrib.patterns.quorumconsensus;

import distrib.patterns.common.*;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;
import distrib.patterns.requests.VersionedSetValueRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

interface Replica {
    void handleClientRequest(Message<RequestOrResponse> message);
    void handleServerMessage(Message<RequestOrResponse> message);
    void setNode(Node node);
}

class QuorumKV implements Replica {
    private static Logger logger = LogManager.getLogger(QuorumKV.class);
    private final ClientState clientState;
    private Node node;
    private int noOfReplicas;
    private boolean doSyncReadRepair;
    private final RequestWaitingList requestWaitingList;

    public QuorumKV(SystemClock clock , boolean doSyncReadRepair) {
        this.clientState = new ClientState(clock);
        this.doSyncReadRepair = doSyncReadRepair;
        this.requestWaitingList = new RequestWaitingList(clock);
    }

    public void setNode(Node node) {
        this.node = node;
        this.noOfReplicas = node.getNoOfReplicas();
    }

    private <T> T deserialize(RequestOrResponse request, Class<T> clazz) {
        return JsonSerDes.deserialize(request.getMessageBodyJson(), clazz);
    }

    public void handleClientRequest(Message<RequestOrResponse> message) {
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
        sendRequestToReplicas(versionCallback, RequestId.GetVersion, getVersion);

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
        WriteQuorumCallback quorumCallback = new WriteQuorumCallback(node.getNoOfReplicas(), clientConnection, correlationId);
        sendRequestToReplicas(quorumCallback, RequestId.SetValueRequest, requestToReplicas);
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
        ReadQuorumCallback quorumCallback = new ReadQuorumCallback(this, noOfReplicas, clientConnection, correlationId, generation, doSyncReadRepair);
        sendRequestToReplicas(quorumCallback, requestId, request);
    }

    public synchronized void handleServerMessage(Message<RequestOrResponse> message) {
        RequestOrResponse requestOrResponse = message.getRequest();

        if (requestOrResponse.getRequestId() == RequestId.GetVersion.getId()) {
            handleGetVersionRequest(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.SetValueRequest.getId()) {
            handleSetValueRequest(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.GetValueRequest.getId()) {
            handleGetValueRequest(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.SetValueResponse.getId()) {
            handleResponse(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.GetValueResponse.getId()) {
            handleResponse(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.GetVersionResponse.getId()) {
            handleResponse(requestOrResponse);
        }
    }

    private void handleGetVersionRequest(RequestOrResponse request) {
        try {
            GetVersionRequest getVersionRequest = deserialize(request, GetVersionRequest.class);
            StoredValue storedValue = node.get(getVersionRequest.getKey());
            MonotonicId version = (storedValue == null) ? MonotonicId.empty() : storedValue.getVersion();
            byte[] serialize = JsonSerDes.serialize(version);
            node.send(request.getFromAddress(), new RequestOrResponse(request.getGeneration(), RequestId.GetVersionResponse.getId(),
                    serialize, request.getCorrelationId(), node.getPeerConnectionAddress()));
        } catch (Exception e) {
            e.printStackTrace();;
        }
    }

    private void handleGetValueRequest(RequestOrResponse request) {
        GetValueRequest getValueRequest = deserialize(request, GetValueRequest.class);
        node.send(request.getFromAddress(),
                new RequestOrResponse(request.getGeneration(),
                        RequestId.GetValueResponse.getId(),
                        JsonSerDes.serialize(node.get(getValueRequest.getKey())),
                        request.getCorrelationId(), node.getPeerConnectionAddress()));
    }

    private void handleResponse(RequestOrResponse response) {
        requestWaitingList.handleResponse(response.getCorrelationId(), response);
    }

    private void handleSetValueRequest(RequestOrResponse request) {
        int maxKnownGeneration = node.maxKnownGeneration();
        Integer requestGeneration = request.getGeneration();
        if (requestGeneration < maxKnownGeneration) {
            String errorMessage = "Rejecting request from generation " + requestGeneration + " as already accepted from generation " + maxKnownGeneration;
            node.send(request.getFromAddress(),
                    new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(),
                            errorMessage.getBytes(), request.getCorrelationId(), node.getPeerConnectionAddress()));
            return;
        }

        //TODO: Assignment 3 Add check for generation while handling requests.
        VersionedSetValueRequest setValueRequest = deserialize(request, VersionedSetValueRequest.class);
        StoredValue storedValue = node.get(setValueRequest.getKey());
        if (setValueRequest.getVersion().isAfter(storedValue.getVersion())) { //set only if setting with higher version timestamp.
            node.put(setValueRequest.getKey(), new StoredValue(setValueRequest.getKey(), setValueRequest.getValue(), setValueRequest.getVersion(), requestGeneration));
        }
        node.send(request.getFromAddress(),
                new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(), "Success".getBytes(),
                        request.getCorrelationId(),
                        node.getPeerConnectionAddress()));
    }

    public <T> void sendRequestToReplicas(RequestCallback quorumCallback, RequestId requestId, T requestToReplicas) {
        for (InetAddressAndPort replica : node.getReplicas()) {
            int correlationId = nextRequestId();
            RequestOrResponse request = new RequestOrResponse(node.getGeneration(), requestId.getId(), JsonSerDes.serialize(requestToReplicas), correlationId, node.getPeerConnectionAddress());
            requestWaitingList.add(request.getCorrelationId(), quorumCallback);
            node.send(replica, request);
        }
    }

    public void sendRequestToReplica(RequestCallback requestCallback, InetAddressAndPort replicaAddress, RequestOrResponse request) {
        requestWaitingList.add(request.getCorrelationId(), requestCallback);
        node.send(replicaAddress, request);
    }

    int requestNumber;
    private int nextRequestId() {
        return requestNumber++;
    }

    public InetAddressAndPort getPeerConnectionAddress() {
        return node.getPeerConnectionAddress();
    }
}
