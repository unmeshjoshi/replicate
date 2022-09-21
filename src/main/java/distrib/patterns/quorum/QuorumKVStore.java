package distrib.patterns.quorum;

import distrib.patterns.common.*;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;
import distrib.patterns.wal.DurableKVStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class QuorumKVStore extends Replica {
    private static Logger logger = LogManager.getLogger(QuorumKVStore.class);
    public static final int firstGeneration = 1;
    private final Config config;
    private final int generation;
    private final List<InetAddressAndPort> replicas;
    private final ClientState clientState;

    //zookeeper/etcd
    private final DurableKVStore systemStorage;
    private final DurableKVStore durableStore;

    public QuorumKVStore(Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> replicas) throws IOException {
        super(config, clock, clientConnectionAddress, peerConnectionAddress, replicas);
        this.config = config;
        this.replicas = replicas;
        //FIXME
        Config configWithSystemWalDir = makeNewConfigWithSystemWalDir(config);
        this.systemStorage = new DurableKVStore(configWithSystemWalDir);
        this.durableStore = new DurableKVStore(config);
        this.generation = incrementAndGetGeneration();
        this.clientState = new ClientState(clock);
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
            handleGetValueRequest(message.getClientConnection(), request.getCorrelationId(), getGeneration(), RequestId.GetValueRequest, clientGetValueRequest);
        }
    }

    ///t1 = clientState.getTimestamp()
    //t2 = clientState.getTimestamp()
    //t3 = clientState.getTimestamp()
    //t3 > t2 > t1 //NTP..  t1 > t3..
    private void handleSetValueRequest(ClientConnection clientConnection, Integer correlationId, SetValueRequest clientSetValueRequest) {
        SetValueRequest requestToReplicas = new SetValueRequest(clientSetValueRequest.getKey(),
                clientSetValueRequest.getValue(),
                clientSetValueRequest.getClientId(),
                clientSetValueRequest.getRequestNumber(),
                clientState.getTimestamp()); //assign timestamp to request.
        WriteQuorumCallback quorumCallback = new WriteQuorumCallback(getNoOfReplicas(), clientConnection, correlationId);
        sendRequestToReplicas(quorumCallback, RequestId.SetValueRequest, requestToReplicas);
    }


    private void handleGetValueRequest(ClientConnection clientConnection, Integer correlationId, int generation, RequestId requestId, GetValueRequest clientSetValueRequest) {
        GetValueRequest request = new GetValueRequest(clientSetValueRequest.getKey());
        ReadQuorumCallback quorumCallback = new ReadQuorumCallback(this, getNoOfReplicas(), clientConnection, correlationId, generation, config.doSyncReadRepair());
        sendRequestToReplicas(quorumCallback, requestId, request);
    }


    private Config makeNewConfigWithSystemWalDir(Config config) {
        String systemWalDir = makeSystemWalDir(config);
        return new Config(systemWalDir);
    }

    private String makeSystemWalDir(Config config) {
        String systemWalDir = config.getWalDir() + "_System";
        new File(systemWalDir).mkdirs();
        return systemWalDir;
    }

    private int incrementAndGetGeneration() {
        String s = systemStorage.get("generation");
        int currentGeneration = s == null? firstGeneration :Integer.parseInt(s) + 1;
        systemStorage.put("generation", String.valueOf(currentGeneration));
        return currentGeneration;
    }

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

    public int maxKnownGeneration() {
        return durableStore.values().stream().map(kv -> JsonSerDes.fromJson(kv.getBytes(), StoredValue.class))
                .map(v -> v.generation).max(Integer::compare).orElse(-1);
    }

    public Config getConfig() {
        return config;
    }

    public int getGeneration() {
        return generation;
    }

    @Override
    public synchronized void handleServerMessage(Message<RequestOrResponse> message) {
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
        send(request.getFromAddress(), new RequestOrResponse(request.getGeneration(), RequestId.GetValueResponse.getId(), JsonSerDes.serialize(get(getValueRequest.getKey())), request.getCorrelationId(), getPeerConnectionAddress()));
    }

    private void handleResponse(RequestOrResponse response) {
        requestWaitingList.handleResponse(response.getCorrelationId(), response);
    }

    private void handleSetValueRequest(RequestOrResponse request) {
        int maxKnownGeneration = maxKnownGeneration();
        Integer requestGeneration = request.getGeneration();

        //TODO: Assignment 3 Add check for generation while handling requests.

        if (requestGeneration < maxKnownGeneration) {
            String errorMessage = "Rejecting request from generation " + requestGeneration + " as already accepted from generation " + maxKnownGeneration;
            send(request.getFromAddress(), new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(), errorMessage.getBytes(), request.getCorrelationId(), getPeerConnectionAddress()));
            return;
        }

        SetValueRequest setValueRequest = deserialize(request, SetValueRequest.class);

        StoredValue storedValue = get(setValueRequest.getKey());

        if (storedValue.getTimestamp() < setValueRequest.getTimestamp()) { //set only if previous timestamp is less.
            logger.info("Setting newer value " + setValueRequest.getValue());
            put(setValueRequest.getKey(), new StoredValue(setValueRequest.getKey(), setValueRequest.getValue(), setValueRequest.getTimestamp(), requestGeneration));
        } else {
            logger.info("Not setting value " + setValueRequest.getValue() + " because timestamp higher " + storedValue.getTimestamp() + " than request " + setValueRequest.getTimestamp());

        }
        send(request.getFromAddress(), new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(), "Success".getBytes(), request.getCorrelationId(), getPeerConnectionAddress()));
    }
}
