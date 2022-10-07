package distrib.patterns.quorum;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.quorum.messages.*;
import distrib.patterns.wal.DurableKVStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A simple key value store with replication handled using 'quorum'.
 * Each client request is sent to all the replicas.
 * Client communicate to the node by request-response rpc.(They expect
 * and wait for the response on the same connection)
 * Replicas communicate by message passing.
 * Each node sends a message to other node, but does not block.
 * When the sending node receives a response message from the other node,
 * it completes the pending requests.
 *
 *
 *                                 message1      +--------+
 *                                +-------------->+        |
 *                                |               | node2  |
 *                                |    +-message2-+        |
 *                                |    |          |        |
 *                             +--+----v+         +--------+
 * +------+   request-response |        |
 * |      |                    |node1   |
 * |client| <--------------->  |        |
 * |      |                    |        |
 * +------+                    +-+----+-+
 *                               |    ^            +---------+
 *                               |    |            |         |
 *                               |    +--message4--+ node3   |
 *                               |                 |         |
 *                               +--message3------->         |
 *                                                 +---------+
 */
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

    public QuorumKVStore(String name, Config config, SystemClock clock, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> replicas) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, replicas);
        this.config = config;
        this.replicas = replicas;
        //TODO:Configure system directory in the client.
        Config configWithSystemWalDir = makeNewConfigWithSystemWalDir(config);
        this.systemStorage = new DurableKVStore(configWithSystemWalDir);
        this.durableStore = new DurableKVStore(config);
        this.generation = incrementAndGetGeneration();
        this.clientState = new ClientState(clock);
    }

    @Override
    protected void registerHandlers() {
        handlesMessage(RequestId.VersionedSetValueRequest, this::handleSetValueRequest, VersionedSetValueRequest.class)
                .respondsWithMessage(RequestId.SetValueResponse, SetValueResponse.class);
        handlesMessage(RequestId.VersionedGetValueRequest, this::handleGetValueRequest, GetValueRequest.class)
                .respondsWithMessage(RequestId.GetValueResponse, GetValueResponse.class);

        handlesRequestAsync(RequestId.SetValueRequest, this::handleClientSetValueRequest, SetValueRequest.class);
        handlesRequestAsync(RequestId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);
    }

    ///t1 = clientState.getTimestamp()
    //t2 = clientState.getTimestamp()
    //t3 = clientState.getTimestamp()
    //t3 > t2 > t1 //NTP..  t1 > t3..
    private CompletableFuture<SetValueResponse> handleClientSetValueRequest(SetValueRequest clientSetValueRequest) {
        VersionedSetValueRequest requestToReplicas = new VersionedSetValueRequest(clientSetValueRequest.getKey(),
                clientSetValueRequest.getValue(),
                clientSetValueRequest.getClientId(),
                clientSetValueRequest.getRequestNumber(),
                clientState.getTimestamp()); //assign timestamp to request.
        AsyncQuorumCallback<String> quorumCallback = new AsyncQuorumCallback<String>(getNoOfReplicas());
        sendMessageToReplicas(quorumCallback, RequestId.VersionedSetValueRequest, requestToReplicas);
        return quorumCallback.getQuorumFuture().thenApply(r -> new SetValueResponse("Success")); //TODO:Map quorum responses to
    }


    private CompletableFuture<StoredValue> handleClientGetValueRequest(GetValueRequest clientSetValueRequest) {
        GetValueRequest request = new GetValueRequest(clientSetValueRequest.getKey());
        AsyncQuorumCallback<GetValueResponse> quorumCallback = new AsyncQuorumCallback<GetValueResponse>(getNoOfReplicas());
        sendMessageToReplicas(quorumCallback, RequestId.VersionedGetValueRequest, request);
        return quorumCallback.getQuorumFuture().thenComposeAsync((responses) -> {
            return new ReadRepairer(this, responses, config.doAsyncReadRepair()).readRepair();
        });
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


    private GetValueResponse handleGetValueRequest(GetValueRequest getValueRequest) {
        StoredValue storedValue = get(getValueRequest.getKey());
        logger.info("Getting value for " + getValueRequest.getKey() + " :" + storedValue);
        return new GetValueResponse(storedValue);
    }

    private SetValueResponse handleSetValueRequest(VersionedSetValueRequest setValueRequest) {
//TODO: Figure out way to handle generation.
//        int maxKnownGeneration = maxKnownGeneration();
//        Integer requestGeneration = request.getGeneration();
//
//        //TODO: Assignment 3 Add check for generation while handling requests.
//
//        if (requestGeneration < maxKnownGeneration) {
//            String errorMessage = "Rejecting request from generation " + requestGeneration + " as already accepted from generation " + maxKnownGeneration;
//            send(request.getFromAddress(), new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(), errorMessage.getBytes(), request.getCorrelationId(), getPeerConnectionAddress()));
//            return;
//        }
        StoredValue storedValue = get(setValueRequest.getKey());

        if (storedValue.getTimestamp() < setValueRequest.getTimestamp()) { //set only if previous timestamp is less.
            logger.info("Setting newer value " + setValueRequest.getValue());
            put(setValueRequest.getKey(), new StoredValue(setValueRequest.getKey(), setValueRequest.getValue(), setValueRequest.getTimestamp(), 1));
        } else {
            logger.info("Not setting value " + setValueRequest.getValue() + " because timestamp higher " + storedValue.getTimestamp() + " than request " + setValueRequest.getTimestamp());

        }
        return new SetValueResponse("Success");
    }
}
