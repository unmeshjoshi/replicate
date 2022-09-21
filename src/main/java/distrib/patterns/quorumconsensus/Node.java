package distrib.patterns.quorumconsensus;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.wal.DurableKVStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Node {
    public static final int firstGeneration = 1;
    private static Logger logger = LogManager.getLogger(Node.class);
    private final Replica replica;
    private final Config config;
    private final int generation;
    private final NIOSocketListener peerListener;
    private final NIOSocketListener clientListener;
    private InetAddressAndPort clientConnectionAddress;
    private InetAddressAndPort peerConnectionAddress;
    private final List<InetAddressAndPort> replicas;
    private final DurableKVStore systemStorage;
    private final DurableKVStore durableStore;
    private final Network network = new Network();

    public Node(Config config, InetAddressAndPort clientConnectionAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> replicas, Replica replica) throws IOException {
        this.config = config;
        this.clientConnectionAddress = clientConnectionAddress;
        this.peerConnectionAddress = peerConnectionAddress;
        this.replicas = replicas;

        String systemWalDir = config.getWalDir() + "_System";
        new File(systemWalDir).mkdirs();
        this.peerListener = new NIOSocketListener(replica::handleServerMessage, peerConnectionAddress);
        this.peerConnectionAddress = peerConnectionAddress;
        this.clientConnectionAddress = clientConnectionAddress;
        this.clientListener = new NIOSocketListener(replica::handleClientRequest, clientConnectionAddress);
        this.systemStorage = new DurableKVStore(new Config(systemWalDir));
        this.durableStore = new DurableKVStore(config);
        this.generation = incrementAndGetGeneration();
        this.replica = replica;
        replica.setNode(this);
    }

    public void start() {
        peerListener.start();
        clientListener.start();
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


    private int incrementAndGetGeneration() {
        String s = systemStorage.get("generation");
        int currentGeneration = s == null? firstGeneration :Integer.parseInt(s) + 1;
        systemStorage.put("generation", String.valueOf(currentGeneration));
        return currentGeneration;
    }

    public int maxKnownGeneration() {
        return durableStore.values().stream().map(kv -> JsonSerDes.fromJson(kv.getBytes(), StoredValue.class))
                .map(v -> v.generation).max(Integer::compare).orElse(-1);
    }

    void send(InetAddressAndPort fromAddress, RequestOrResponse message) {
        try {
            network.sendOneWay(fromAddress, message);
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + fromAddress);
        }
    }

    public Config getConfig() {
        return config;
    }

    public int getGeneration() {
        return generation;
    }

    public InetAddressAndPort getClientConnectionAddress() {
        return clientConnectionAddress;
    }

    public InetAddressAndPort getPeerConnectionAddress() {
        return peerConnectionAddress;
    }

    public List<InetAddressAndPort> getReplicas() {
        return replicas;
    }

    int getNoOfReplicas() {
        return getReplicas().size();
    }

    public void dropMessagesTo(Node byzantium) {
        network.dropMessagesTo(byzantium.getPeerConnectionAddress());
    }

    public void reconnectTo(Node cyrene) {
        network.reconnectTo(cyrene.getPeerConnectionAddress());
    }

    public void dropMessagesToAfter(Node byzantium, int dropAfterNoOfMessages) {
        network.dropMessagesAfter(byzantium.getPeerConnectionAddress(), dropAfterNoOfMessages);
    }

    public MonotonicId getVersion(String key) {
        StoredValue storedValue = get(key);
        if (storedValue == null) {
            return MonotonicId.empty();
        }
        return storedValue.getVersion();
    }
}
