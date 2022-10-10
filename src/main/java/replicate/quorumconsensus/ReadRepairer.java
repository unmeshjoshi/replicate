package replicate.quorumconsensus;

import distrib.patterns.common.*;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.quorumconsensus.messages.GetValueResponse;
import replicate.quorumconsensus.messages.VersionedSetValueRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class ReadRepairer {
    static Logger logger = LogManager.getLogger(ReadRepairer.class);
    private Replica replica;
    private Map<InetAddressAndPort, GetValueResponse> nodesToValues;

    public ReadRepairer(Replica replica, Map<InetAddressAndPort, GetValueResponse> nodesToValues) {
        this.replica = replica;
        this.nodesToValues = nodesToValues;
    }

    public CompletableFuture<StoredValue> readRepair() {
        StoredValue latestStoredValue = getLatestStoredValue();
        return readRepair(latestStoredValue);
    }

    private CompletableFuture<StoredValue> readRepair(StoredValue latestStoredValue) {
        var nodesHavingStaleValues = getNodesHavingStaleValues(latestStoredValue.getVersion());
        if (nodesHavingStaleValues.isEmpty()) {
            return CompletableFuture.completedFuture(latestStoredValue);
        }
        var writeRequest = createSetValueRequest(latestStoredValue.getKey(), latestStoredValue.getValue(), latestStoredValue.getVersion());
        var requestCallback = new AsyncQuorumCallback<String>(nodesHavingStaleValues.size());
        for (InetAddressAndPort nodesHavingStaleValue : nodesHavingStaleValues) {
            logger.info("Sending read repair request to " + nodesHavingStaleValue + ":" + latestStoredValue.getValue());
            replica.sendMessageToReplica(requestCallback, nodesHavingStaleValue, writeRequest);
        }
        return requestCallback.getQuorumFuture()
                .thenApply((result) -> latestStoredValue);
    }

    int requestId;

    private RequestOrResponse createSetValueRequest(String key, String value, MonotonicId timestamp) {
        VersionedSetValueRequest setValueRequest = new VersionedSetValueRequest(key, value, timestamp);
        RequestOrResponse requestOrResponse = new RequestOrResponse(RequestId.VersionedSetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), requestId++, replica.getPeerConnectionAddress());
        return requestOrResponse;
    }

    private List<InetAddressAndPort> getNodesHavingStaleValues(MonotonicId latestTimestamp) {
        return this.nodesToValues.entrySet().stream().filter(e -> latestTimestamp.isAfter(e.getValue().getValue().getVersion())).map(e -> e.getKey()).collect(Collectors.toList());
    }

    private StoredValue getLatestStoredValue() {
        return this.nodesToValues.values().stream().map(r -> r.getValue()).max(Comparator.comparing(StoredValue::getVersion)).orElse(StoredValue.EMPTY);
    }
}
