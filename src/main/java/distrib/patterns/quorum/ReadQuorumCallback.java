package distrib.patterns.quorum;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.RequestId;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.requests.SetValueRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class ReadQuorumCallback extends QuorumCallback {
    static Logger logger = LogManager.getLogger(ReadQuorumCallback.class);

    Map<InetAddressAndPort, StoredValue> responses = new HashMap<>();
    private QuorumKVStore kvStore;
    private Integer generation;
    private boolean doSyncReadRepair;

    public ReadQuorumCallback(QuorumKVStore kvStore, int totalExpectedResponses, ClientConnection clientConnection, Integer correlationId, Integer generation, boolean doSyncReadRepair) {
        super(totalExpectedResponses, clientConnection, correlationId);
        this.kvStore = kvStore;
        this.generation = generation;
        this.doSyncReadRepair = doSyncReadRepair;
    }

    @Override
    public void onResponse(RequestOrResponse response) {
        mapResponsesFromReplicas(response);
        super.onResponse(response);
    }

    private void mapResponsesFromReplicas(RequestOrResponse response) {
        StoredValue kvResponse = JsonSerDes.deserialize(response.getMessageBodyJson(), StoredValue.class);
        responses.put(response.getFromAddress(), kvResponse);
    }

    @Override
    CompletableFuture<String> processQuorumResponses() {
        return readRepair();
    }

    private CompletableFuture readRepair() {
        StoredValue latestStoredValue = getLatestStoredValue();
        return readRepair(latestStoredValue);
    }

    private CompletableFuture<String> readRepair(StoredValue latestStoredValue) {
        List<InetAddressAndPort> nodesHavingStaleValues = getNodesHavingStaleValues(latestStoredValue.getTimestamp());
        RequestOrResponse writeRequest = createSetValueRequest(latestStoredValue.getKey(), latestStoredValue.getValue(), latestStoredValue.getTimestamp(), generation);
        WaitingRequestCallback requestCallback = new WaitingRequestCallback(nodesHavingStaleValues.size());
        for (InetAddressAndPort nodesHavingStaleValue : nodesHavingStaleValues) {
            logger.info("Sending read repair request to " + nodesHavingStaleValue + ":" + latestStoredValue.getValue());
            kvStore.sendRequestToReplica(requestCallback, nodesHavingStaleValue, writeRequest);
        }

        if (doSyncReadRepair) {
            logger.info("Waiting for read repair");
            return CompletableFuture.supplyAsync(()->{
                if (requestCallback.await(Duration.ofSeconds(2))){
                    return latestStoredValue.getValue();
                };
                return "Error";//FIXME
            });

        } else {
            return CompletableFuture.completedFuture(latestStoredValue.getValue());
        }
    }

    int requestId;
    private RequestOrResponse createSetValueRequest(String key, String value, long timestamp, Integer generation) {
        SetValueRequest setValueRequest = new SetValueRequest(key, value, -1, -1, timestamp);
        RequestOrResponse requestOrResponse = new RequestOrResponse(generation, RequestId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), requestId++, kvStore.getPeerConnectionAddress());
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

}
