package distrib.patterns.quorumconsensus;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.quorumconsensus.messages.*;
import distrib.patterns.wal.DurableKVStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class QuorumKV extends Replica {
    private static Logger logger = LogManager.getLogger(QuorumKV.class);

    public QuorumKV(Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, boolean doSyncReadRepair, List<InetAddressAndPort> peers) throws IOException {
        super(config, clock,clientConnectionAddress, peerConnectionAddress, peers);
        this.durableStore = new DurableKVStore(config);
        //messagehandler(RequestId.GetVersion, this::handleGetVersionRequest, GetVersionRequest.class)
        // .respondedWith(RequestId.GetVersionResponse, GetVersionResponse.class)
        messageHandler(RequestId.GetVersion, this::handleGetVersionRequest, GetVersionRequest.class);
        responseMessageHandler(RequestId.GetVersionResponse, GetVersionResponse.class);

        messageHandler(RequestId.VersionedSetValueRequest, this::handlePeerSetValueRequest, VersionedSetValueRequest.class);
        responseMessageHandler(RequestId.SetValueResponse, SetValueResponse.class);

        messageHandler(RequestId.VersionedGetValueRequest, this::handleGetValueRequest, GetValueRequest.class);
        responseMessageHandler(RequestId.GetValueResponse, GetValueResponse.class);

        requestHandler(RequestId.SetValueRequest, this::handleClientSetValueRequest, SetValueRequest.class);
        requestHandler(RequestId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);
    }

    private CompletableFuture<SetValueResponse> handleClientSetValueRequest(SetValueRequest clientSetValueRequest) {
        var getVersion = new GetVersionRequest(clientSetValueRequest.getKey());
        var versionCallback = new AsyncQuorumCallback<GetVersionResponse>(getNoOfReplicas());
        sendRequestToReplicas(versionCallback, RequestId.GetVersion, getVersion);
        var quorumFuture = versionCallback.getQuorumFuture();
        return quorumFuture.thenCompose((r) ->
                assignVersionAndSetValue(clientSetValueRequest, r.values().stream().toList()));
    }

    private CompletableFuture<SetValueResponse> assignVersionAndSetValue(SetValueRequest clientSetValueRequest, List<GetVersionResponse> existingVersions) {
        VersionedSetValueRequest requestToReplicas = new VersionedSetValueRequest(clientSetValueRequest.getKey(),
                clientSetValueRequest.getValue(),
                clientSetValueRequest.getClientId(),
                clientSetValueRequest.getRequestNumber(),
                getNextId(existingVersions.stream().map(r -> r.getVersion()).collect(Collectors.toList()))
        ); //assign timestamp to request.
        var quorumCallback = new AsyncQuorumCallback<SetValueResponse>(getNoOfReplicas());
        sendRequestToReplicas(quorumCallback, RequestId.VersionedSetValueRequest, requestToReplicas);
        CompletableFuture<Map<InetAddressAndPort, SetValueResponse>> quorumFuture = quorumCallback.getQuorumFuture();
        return quorumFuture.thenApply(r -> {
            //TODO:Find how to handle multiple values;
            return r.values().stream().findFirst().get();
        });
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

    private CompletableFuture<StoredValue> handleClientGetValueRequest(GetValueRequest request) {
        var asyncQuorumCallback = new AsyncQuorumCallback<GetValueResponse>(getNoOfReplicas());
        sendRequestToReplicas(asyncQuorumCallback, RequestId.VersionedGetValueRequest, request);
        return asyncQuorumCallback.getQuorumFuture()
                .thenCompose((nodesToValues)-> {
                    return new ReadRepairer(this, nodesToValues).readRepair();
                });
    }

    private GetVersionResponse handleGetVersionRequest(GetVersionRequest getVersionRequest) {
        StoredValue storedValue = get(getVersionRequest.getKey());
        MonotonicId version = (storedValue == null) ? MonotonicId.empty() : storedValue.getVersion();
        return new GetVersionResponse(version);
    }

    private GetValueResponse handleGetValueRequest(GetValueRequest getValueRequest) {
        StoredValue storedValue = get(getValueRequest.getKey());
        return new GetValueResponse(storedValue);
    }

    private SetValueResponse handlePeerSetValueRequest(VersionedSetValueRequest setValueRequest) {
        StoredValue storedValue = get(setValueRequest.getKey());
        if (setValueRequest.getVersion().isAfter(storedValue.getVersion())) { //set only if setting with higher version timestamp.
            put(setValueRequest.getKey(), new StoredValue(setValueRequest.getKey(), setValueRequest.getValue(), setValueRequest.getVersion()));
        }
        return new SetValueResponse("Success");
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
