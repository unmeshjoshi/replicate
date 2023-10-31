package replicate.quorum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.quorum.messages.*;
import replicate.wal.DurableKVStore;

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
        //messages handled by replicas.
        handlesMessage(MessageId.VersionedSetValueRequest, this::handleSetValueRequest, VersionedSetValueRequest.class);
        handlesMessage(MessageId.SetValueResponse, this::handleSetValueResponse, SetValueResponse.class);
        handlesMessage(MessageId.VersionedGetValueRequest, this::handleGetValueRequest, GetValueRequest.class);
        handlesMessage(MessageId.GetValueResponse, this::handleGetValueResponse, GetValueResponse.class);

        //client requests
        //These handles send messages to replicas using Replica::sendMessageToReplicas
        //It uses RequestWaitingList to wait for the corresponding response messages.
        handlesRequestAsync(MessageId.SetValueRequest, this::handleClientSetValueRequest, SetValueRequest.class);
        handlesRequestAsync(MessageId.GetValueRequest, this::handleClientGetValueRequest, GetValueRequest.class);
    }

    private void handleGetValueResponse(Message<GetValueResponse> message) {
        handleResponse(message);
    }

    private void handleSetValueResponse(Message<SetValueResponse> message) {
        handleResponse(message);
    }

    ///t1 = clientState.getTimestamp()
    //t2 = clientState.getTimestamp()
    //t3 = clientState.getTimestamp()
    //t3 > t2 > t1 //NTP..  t1 > t3..
    //LWW - last write wins implementation - provided clocks are in sync.
    private CompletableFuture<SetValueResponse> handleClientSetValueRequest(SetValueRequest clientSetValueRequest) {
        VersionedSetValueRequest requestToReplicas = new VersionedSetValueRequest(clientSetValueRequest.getKey(),
                clientSetValueRequest.getValue(),
                clientSetValueRequest.getClientId(),
                clientSetValueRequest.getRequestNumber(),
                clientState.getTimestamp()); //assign timestamp to request.
        AsyncQuorumCallback<String> quorumCallback = new AsyncQuorumCallback<String>(getNoOfReplicas());
        sendMessageToReplicas(quorumCallback, MessageId.VersionedSetValueRequest, requestToReplicas);
        return quorumCallback.getQuorumFuture().thenApply(r -> new SetValueResponse("Success"));
    }


    private CompletableFuture<GetValueResponse> handleClientGetValueRequest(GetValueRequest clientRequest) {
        logger.info("Handling get request for " + clientRequest.getKey() + " in " + getName());
        GetValueRequest requestToReplicas = new GetValueRequest(clientRequest.getKey());
        AsyncQuorumCallback<GetValueResponse> quorumCallback = new AsyncQuorumCallback<GetValueResponse>(getNoOfReplicas());
        sendMessageToReplicas(quorumCallback, MessageId.VersionedGetValueRequest, requestToReplicas);
        return quorumCallback.getQuorumFuture().thenComposeAsync((responses) -> {
            return new ReadRepairer(this, responses, config.isAsyncReadRepair()).readRepair();
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

    public String getValue(String key) {
        return get(key).value;
    }

    public Config getConfig() {
        return config;
    }

    public int getGeneration() {
        return generation;
    }


    private void handleGetValueRequest(Message<GetValueRequest> message) {
        GetValueRequest getValueRequest = message.messagePayload();
        StoredValue storedValue = get(getValueRequest.getKey());
        logger.info("Getting value for " + getValueRequest.getKey() + " :" + storedValue + " from " + getName());
        sendOneway(message.getFromAddress(), new GetValueResponse(storedValue), message.getCorrelationId());
    }

    private void handleSetValueRequest(Message<VersionedSetValueRequest> message) {
        var setValueRequest = message.messagePayload();
        StoredValue storedValue = get(setValueRequest.key);

        if (storedValue.timestamp < setValueRequest.version) { //set only if previous timestamp is less.
            logger.info("Setting newer value " + setValueRequest.value);
            put(setValueRequest.key, new StoredValue(setValueRequest.key, setValueRequest.value, setValueRequest.version, 1));
        } else {
            logger.info("Not setting value " + setValueRequest.value + " because timestamp higher " + storedValue.timestamp + " than request " + setValueRequest.version);

        }
        sendOneway(message.getFromAddress(), new SetValueResponse("Success"), message.getCorrelationId());
    }

    public void doAsyncReadRepair() {
        config.setAsyncReadRepair();
    }

    @Override
    public void setClock(SystemClock clock) {
        this.clientState.clock = clock;
    }
}
