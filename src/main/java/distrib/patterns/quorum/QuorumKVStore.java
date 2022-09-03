package distrib.patterns.quorum;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.wal.DurableKVStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class QuorumKVStore {
    public static final int firstGeneration = 1;
    private static Logger logger = LogManager.getLogger(QuorumKVStore.class);
    private final ClientRequestHandler clientRequestHandler;
    private final PeerMessagingService peerMessagingService;
    private final Config config;
    private final int generation;
    private final List<InetAddressAndPort> replicas;

    //zookeeper/etcd
    private final DurableKVStore systemStorage;


    private final DurableKVStore durableStore;

    public QuorumKVStore(SystemClock clock, Config config, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> replicas) throws IOException {
        this.config = config;
        this.replicas = replicas;

        //FIXME
        String systemWalDir = config.getWalDir() + "_System";
        new File(systemWalDir).mkdirs();
        this.systemStorage = new DurableKVStore(new Config(systemWalDir));

        this.durableStore = new DurableKVStore(config);
        this.generation = incrementAndGetGeneration();
        this.clientRequestHandler = new ClientRequestHandler(clientConnectionAddress, clock, this, config.doSyncReadRepair());
        this.peerMessagingService = new PeerMessagingService(peerConnectionAddress, this, clock);
    }

    private int incrementAndGetGeneration() {
        String s = systemStorage.get("generation");
        int currentGeneration = s == null? firstGeneration :Integer.parseInt(s) + 1;
        systemStorage.put("generation", String.valueOf(currentGeneration));
        return currentGeneration;
    }

    public void start() {
        peerMessagingService.start();
        clientRequestHandler.start();
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


    public <T> void sendRequestToReplicas(QuorumCallback quorumCallback, RequestId requestId, T requestToReplicas) {
        peerMessagingService.sendRequestToReplicas(quorumCallback, requestId, requestToReplicas);
    }

    public void sendRequestToReplica(RequestCallback requestCallback, InetAddressAndPort replicaAddress, RequestOrResponse request) {
        peerMessagingService.sendRequestToReplica(requestCallback, replicaAddress, request);
    }



    public void dropMessagesTo(QuorumKVStore kvStore) {
        peerMessagingService.dropMessagesTo(kvStore);
    }

    public void reconnectTo(QuorumKVStore kvStore) {
        peerMessagingService.reconnectTo(kvStore);
    }

    public void addDelayForMessagesTo(QuorumKVStore kvstore, int afterNoOfMessages) {
        peerMessagingService.addDelayForMessagesTo(kvstore, afterNoOfMessages);
    }

    public Config getConfig() {
        return config;
    }

    public int getGeneration() {
        return generation;
    }

    public InetAddressAndPort getClientConnectionAddress() {
        return clientRequestHandler.getClientConnectionAddress();
    }

    public InetAddressAndPort getPeerConnectionAddress() {
        return peerMessagingService.getPeerConnectionAddress();
    }

    public List<InetAddressAndPort> getReplicas() {
        return replicas;
    }

    int getNoOfReplicas() {
        return getReplicas().size();
    }

    public void dropMessagesToAfterNoOfCalls(QuorumKVStore byzantium, int dropAfterNoOfMessages) {
        peerMessagingService.dropMessagesAfter(byzantium, dropAfterNoOfMessages);
    }

    public void delayMessagesTo(QuorumKVStore cyrene, int noOfMessages) {
        peerMessagingService.addDelayForMessagesTo(cyrene, noOfMessages);
    }
}
