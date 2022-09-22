package distrib.patterns.quorumconsensus;

import distrib.patterns.common.*;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;
import distrib.patterns.requests.VersionedSetValueRequest;
import distrib.patterns.wal.DurableKVStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

class QuorumKV extends Replica {
    private static Logger logger = LogManager.getLogger(QuorumKV.class);
    private final ClientState clientState;
    private boolean doSyncReadRepair;

    public QuorumKV(Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, boolean doSyncReadRepair, List<InetAddressAndPort> peers) throws IOException {
        super(config, clock,clientConnectionAddress, peerConnectionAddress, peers);
        this.clientState = new ClientState(clock);
        this.doSyncReadRepair = doSyncReadRepair;
        this.durableStore = new DurableKVStore(config);
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
        WriteQuorumCallback quorumCallback = new WriteQuorumCallback(getNoOfReplicas(), clientConnection, correlationId);
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
        ReadQuorumCallback quorumCallback = new ReadQuorumCallback(this, getNoOfReplicas(), clientConnection, correlationId, generation, doSyncReadRepair);
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
            StoredValue storedValue = get(getVersionRequest.getKey());
            MonotonicId version = (storedValue == null) ? MonotonicId.empty() : storedValue.getVersion();
            byte[] serialize = JsonSerDes.serialize(version);
            send(request.getFromAddress(), new RequestOrResponse(request.getGeneration(), RequestId.GetVersionResponse.getId(),
                    serialize, request.getCorrelationId(), getPeerConnectionAddress()));
        } catch (Exception e) {
            e.printStackTrace();;
        }
    }

    private void handleGetValueRequest(RequestOrResponse request) {
        GetValueRequest getValueRequest = deserialize(request, GetValueRequest.class);
        send(request.getFromAddress(),
                new RequestOrResponse(request.getGeneration(),
                        RequestId.GetValueResponse.getId(),
                        JsonSerDes.serialize(get(getValueRequest.getKey())),
                        request.getCorrelationId(), getPeerConnectionAddress()));
    }

    private void handleSetValueRequest(RequestOrResponse request) {
        VersionedSetValueRequest setValueRequest = deserialize(request, VersionedSetValueRequest.class);
        StoredValue storedValue = get(setValueRequest.getKey());
        if (setValueRequest.getVersion().isAfter(storedValue.getVersion())) { //set only if setting with higher version timestamp.
            put(setValueRequest.getKey(), new StoredValue(setValueRequest.getKey(), setValueRequest.getValue(), setValueRequest.getVersion()));
        }
        send(request.getFromAddress(),
                new RequestOrResponse(RequestId.SetValueResponse.getId(), "Success".getBytes(),
                        request.getCorrelationId(),
                        getPeerConnectionAddress()));
    }


    private final DurableKVStore durableStore;
    public void put(String key, StoredValue storedValue) {
        durableStore.put(key, JsonSerDes.toJson(storedValue));
    }

    public StoredValue get(String key) {
        String storedValue = durableStore.get(key);
        if (storedValue == null) {
            return StoredValue.EMPTY;
        }
        return JsonSerDes.fromJson(storedValue.getBytes(), StoredValue.class);
    }

    public MonotonicId getVersion(String key) {
        StoredValue storedValue = get(key);
        if (storedValue == null) {
            return MonotonicId.empty();
        }
        return storedValue.getVersion();
    }
}
