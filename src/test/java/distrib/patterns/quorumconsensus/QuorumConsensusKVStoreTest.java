package distrib.patterns.quorumconsensus;

import common.TestUtils;
import distrib.patterns.common.Config;
import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.SystemClock;
import distrib.patterns.net.InetAddressAndPort;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class QuorumConsensusKVStoreTest {

    @Test
    public void readRepair() throws IOException {
        List<QuorumKV> kvStores = startCluster(3);
        QuorumKV athens = kvStores.get(0);
        QuorumKV byzantium = kvStores.get(1);
        QuorumKV cyrene = kvStores.get(2);

        athens.dropMessagesTo(byzantium);

        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Success", response);


        MonotonicId id1 = athens.getVersion("title");
        MonotonicId id2 = byzantium.getVersion("title");
        MonotonicId id3 = cyrene.getVersion("title");

        assertEquals(id1, new MonotonicId(1, 1));
        assertEquals(MonotonicId.empty(), id2);
        assertEquals(id3, new MonotonicId(1, 1));

        String title = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Microservices", title);

        assertEquals(new MonotonicId(1, 1),  byzantium.getVersion("title"));
    }

    @Test
    public void compareAndSwapIsSuccessfulForTwoConcurrentClients() throws IOException {
        List<QuorumKV> kvStores = startCluster(3);
        QuorumKV athens = kvStores.get(0);
        QuorumKV byzantium = kvStores.get(1);
        QuorumKV cyrene = kvStores.get(2);

        athens.dropMessagesToAfter(byzantium, 1);
        athens.dropMessagesTo(cyrene);

        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Nitroservices");
        assertEquals("Error", response);
        //quorum responses not received as messages to byzantium and cyrene fail.

        assertEquals(new MonotonicId(1, 1), athens.getVersion("title"));
        assertEquals(new MonotonicId(-1, -1), byzantium.getVersion("title"));
        assertEquals(new MonotonicId(-1, -1), cyrene.getVersion("title"));

        KVClient alice = new KVClient();

        //cyrene should be able to connect with itself and byzantium.
        //both cyrene and byzantium have empty value.
        //Alice starts the compareAndSwap
        //Alice reads the value.
        String aliceValue = alice.getValue(cyrene.getClientConnectionAddress(), "title");

        //meanwhile bob starts compareAndSwap as well
        //Bob connects to athens, which is now able to connect to cyrene and byzantium
        KVClient bob = new KVClient();
        athens.reconnectTo(cyrene);
        athens.reconnectTo(byzantium);
        String bobValue = bob.getValue(athens.getClientConnectionAddress(), "title");
        if (bobValue.equals("Microservices")) {
            kvClient.setValue(athens.getClientConnectionAddress(), "title", "Distributed Systems");
        }
        //Bob successfully completes compareAndSwap

        //Alice checks the value to be empty.
        if (aliceValue.equals("")) {
            alice.setValue(cyrene.getClientConnectionAddress(), "title", "Nitroservices");
        }
        //Alice successfully completes compareAndSwap

        //Bob is surprised to read the different value after his compareAndSwap was successful.
        response = bob.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Distributed Systems", response);
    }


    private List<QuorumKV> startCluster(int clusterSize) throws IOException {
        List<QuorumKV> clusterQuorumKVs = new ArrayList<>();
        SystemClock clock = new SystemClock();
        List<InetAddressAndPort> addresses = TestUtils.createNAddresses(clusterSize);
        List<InetAddressAndPort> clientInterfaceAddresses = TestUtils.createNAddresses(clusterSize);

        for (int i = 0; i < clusterSize; i++) {
            Config config = new Config(TestUtils.tempDir("clusternode_" + i).getAbsolutePath());
            QuorumKV receivingClusterQuorumKV = new QuorumKV(config, clock, clientInterfaceAddresses.get(i), addresses.get(i), true, addresses);
            receivingClusterQuorumKV.start();
            clusterQuorumKVs.add(receivingClusterQuorumKV);
        }
        return clusterQuorumKVs;
    }


}