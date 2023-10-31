package replicate.quorum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.quorum.messages.GetValueResponse;
import replicate.quorum.messages.VersionedSetValueRequest;
import replicate.vsr.CompletionCallback;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class ReadRepairer {
    static Logger logger = LogManager.getLogger(ReadRepairer.class);
    private Replica replica;
    private Map<InetAddressAndPort, GetValueResponse> nodesToValues;

    public ReadRepairer(Replica replica, Map<InetAddressAndPort, GetValueResponse> nodesToValues, boolean isAsyncRepair) {
        this.replica = replica;
        this.nodesToValues = nodesToValues;
        this.isAsyncRepair = isAsyncRepair;
    }

    public CompletableFuture<GetValueResponse> readRepair() {
        StoredValue latestStoredValue = getLatestStoredValue();
        return readRepair(latestStoredValue)
                .thenApply((storedValue) -> new GetValueResponse(storedValue));
    }

    boolean isAsyncRepair;

    private CompletableFuture<StoredValue> readRepair(StoredValue latestStoredValue) {
        var nodesHavingStaleValues = getNodesHavingStaleValues(latestStoredValue.timestamp);
        if (nodesHavingStaleValues.isEmpty()) {
            return CompletableFuture.completedFuture(latestStoredValue);
        }
        List<CompletableFuture<RequestOrResponse>> responseFutures = new ArrayList<>();
        var writeRequest = createSetValueRequest(latestStoredValue.key, latestStoredValue.value, latestStoredValue.timestamp);
        for (InetAddressAndPort nodesHavingStaleValue : nodesHavingStaleValues) {
            var requestCallback = new CompletionCallback();
            logger.info("Sending read repair request to " + nodesHavingStaleValue + ":" + latestStoredValue.value);
            responseFutures.add(requestCallback.getFuture());
            replica.sendMessageToReplica(requestCallback, nodesHavingStaleValue, writeRequest);
        }
        if (isAsyncRepair) {
            return CompletableFuture.completedFuture(latestStoredValue); //complete immidiately.
        } else {
            return Utils.sequence(responseFutures)
                    .thenApply((result) -> {
                        return latestStoredValue;
                    });
        }
    }

    int requestId = new Random().nextInt();
    private RequestOrResponse createSetValueRequest(String key, String value, long timestamp) {
        VersionedSetValueRequest setValueRequest = new VersionedSetValueRequest(key, value, -1, -1, timestamp);
        RequestOrResponse requestOrResponse = new RequestOrResponse(MessageId.VersionedSetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), requestId++, replica.getPeerConnectionAddress());
        return requestOrResponse;
    }

    private List<InetAddressAndPort> getNodesHavingStaleValues(long latestTimestamp) {
        return this.nodesToValues.entrySet().stream().filter(e -> latestTimestamp > (e.getValue().value.timestamp)).map(e -> e.getKey()).collect(Collectors.toList());
    }

    //TODO:assignment
    private StoredValue getLatestStoredValue() {
        return this.nodesToValues.values().stream().map(r -> r.value).max(Comparator.comparingLong(storedValue -> storedValue.timestamp)).orElse(StoredValue.EMPTY);
    }
}
