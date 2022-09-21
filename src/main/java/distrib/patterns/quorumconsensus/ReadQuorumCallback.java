package distrib.patterns.quorumconsensus;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.RequestId;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.requests.SetValueRequest;
import distrib.patterns.requests.VersionedSetValueRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class ReadQuorumCallback extends QuorumCallback {
    static Logger logger = LogManager.getLogger(ReadQuorumCallback.class);

    Map<InetAddressAndPort, StoredValue> responses = new HashMap<>();
    private QuorumKV kvStore;
    private Integer generation;
    private boolean doSyncReadRepair;

    public ReadQuorumCallback(QuorumKV kvStore, int totalExpectedResponses, ClientConnection clientConnection, Integer correlationId, Integer generation, boolean doSyncReadRepair) {
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
        List<InetAddressAndPort> nodesHavingStaleValues = getNodesHavingStaleValues(latestStoredValue.getVersion());
        RequestOrResponse writeRequest = createSetValueRequest(latestStoredValue.getKey(), latestStoredValue.getValue(), latestStoredValue.getVersion(), generation);
        WaitingRequestCallback requestCallback = new WaitingRequestCallback(nodesHavingStaleValues.size());
        for (InetAddressAndPort nodesHavingStaleValue : nodesHavingStaleValues) {
            logger.info("Sending read repair request to " + nodesHavingStaleValue + ":" + latestStoredValue.getValue());
            kvStore.sendRequestToReplica(requestCallback, nodesHavingStaleValue, writeRequest);
        }

        logger.info("Waiting for read repair");
        return CompletableFuture.supplyAsync(() -> {
            if (requestCallback.await(Duration.ofSeconds(2))) {
                return latestStoredValue.getValue();
            }
            ;
            return "Error";//FIXME
        });
    }

    int requestId;

    private RequestOrResponse createSetValueRequest(String key, String value, MonotonicId timestamp, Integer generation) {
        VersionedSetValueRequest setValueRequest = new VersionedSetValueRequest(key, value, -1, -1, timestamp);
        RequestOrResponse requestOrResponse = new RequestOrResponse(generation, RequestId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), requestId++, kvStore.getPeerConnectionAddress());
        return requestOrResponse;
    }

    private List<InetAddressAndPort> getNodesHavingStaleValues(MonotonicId latestTimestamp) {
        return responses.entrySet().stream().filter(e -> latestTimestamp.isAfter(e.getValue().getVersion())).map(e -> e.getKey()).collect(Collectors.toList());
    }

    private StoredValue getLatestStoredValue() {
        return this.responses.values().stream().max(Comparator.comparing(StoredValue::getVersion)).orElse(StoredValue.EMPTY);
    }

}
