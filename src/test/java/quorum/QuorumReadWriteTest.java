package quorum;

import common.TestUtils;
import distrib.patterns.common.Config;
import distrib.patterns.common.SystemClock;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.quorum.QuorumKVStore;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class QuorumReadWriteTest {

    @Test
    public void quorumReadWriteTest() throws IOException {
        List<QuorumKVStore> clusterNodes = startCluster(3);
        QuorumKVStore athens = clusterNodes.get(0);
        QuorumKVStore byzantium = clusterNodes.get(1);
        QuorumKVStore cyrene = clusterNodes.get(2);

        athens.dropMessagesTo(byzantium);

        InetAddressAndPort athensAddress = clusterNodes.get(0).getClientConnectionAddress();
        KVClient kvClient = new KVClient();
        String response = kvClient.setKV(athensAddress, "title", "Microservices");
        assertEquals("Success", response);

        String value = kvClient.getValue(athensAddress, "title");
        assertEquals("Microservices", value);

        assertEquals("Microservices", athens.getStoredValue("title").getValue());
    }

    @Test
    public void quorumReadRepairTest() throws IOException {
        List<QuorumKVStore> clusterNodes = startCluster(3);
        QuorumKVStore athens = clusterNodes.get(0);
        QuorumKVStore byzantium = clusterNodes.get(1);
        QuorumKVStore cyrene = clusterNodes.get(2);

        athens.dropMessagesTo(byzantium);

        KVClient kvClient = new KVClient();
        String response = kvClient.setKV(athens.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Success", response);

        assertEquals("Microservices", athens.getStoredValue("title").getValue());
        assertEquals("Microservices", cyrene.getStoredValue("title").getValue());


        String value = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Microservices", value);
        TestUtils.waitUntilTrue(()-> {
                return "Microservices".equals(byzantium.getStoredValue("title").getValue());
                }, "Waiting for read repair", Duration.ofSeconds(2));

    }
    
    @Test
    public void quorumIncompleteWriteTest() throws IOException {
        List<QuorumKVStore> clusterNodes = startCluster(3);
        QuorumKVStore athens = clusterNodes.get(0);
        QuorumKVStore byzantium = clusterNodes.get(1);
        QuorumKVStore cyrene = clusterNodes.get(2);

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        InetAddressAndPort athensAddress = athens.getClientConnectionAddress();
        KVClient kvClient = new KVClient();
        String response = kvClient.setKV(athensAddress, "title", "Microservices");
        assertEquals("Error", response);

        String value = kvClient.getValue(athensAddress, "title");
        assertEquals("Error", value);

        assertEquals("Microservices", athens.getStoredValue("title").getValue());
    }

    private List<QuorumKVStore> startCluster(int clusterSize) throws IOException {
        List<QuorumKVStore> clusterNodes = new ArrayList<>();
        SystemClock clock = new SystemClock();
        List<InetAddressAndPort> addresses = TestUtils.createNAddresses(clusterSize);
        List<InetAddressAndPort> clientInterfaceAddresses = TestUtils.createNAddresses(clusterSize);

        for (int i = 0; i < clusterSize; i++) {
            Config config = new Config(TestUtils.tempDir("clusternode_" + i).getAbsolutePath());
            QuorumKVStore receivingClusterNode = new QuorumKVStore(clock, config, clientInterfaceAddresses.get(i), addresses.get(i), addresses);
            receivingClusterNode.start();
            clusterNodes.add(receivingClusterNode);
        }
        return clusterNodes;
    }


    @Test
    public void nodesShouldRejectRequestsFromPreviousGenerationNode() throws IOException {
        List<QuorumKVStore> clusterNodes = startCluster(3);
        QuorumKVStore primaryClusterNode = clusterNodes.get(0);
        KVClient client = new KVClient();
        InetAddressAndPort primaryNodeAddress = primaryClusterNode.getClientConnectionAddress();
        assertEquals("Success", client.setKV(primaryNodeAddress, "key", "value"));

        assertEquals("value", client.getValue(primaryNodeAddress, "key"));

        //Simulates starting a new primary instance because the first went under a GC pause.
        Config config = new Config(primaryClusterNode.getConfig().getWalDir().getAbsolutePath());
        InetAddressAndPort newClientAddress = TestUtils.randomLocalAddress();
        QuorumKVStore newInstance = new QuorumKVStore(new SystemClock(), config, newClientAddress, TestUtils.randomLocalAddress(), Arrays.asList(clusterNodes.get(1).getPeerConnectionAddress(), clusterNodes.get(2).getPeerConnectionAddress()));
        newInstance.start();

        assertEquals(2, newInstance.getGeneration());
        assertEquals("Success", client.setKV(newClientAddress, "key1", "value1"));
        assertEquals("Rejecting request from generation 1 as already accepted from generation 2", client.setKV(primaryNodeAddress, "key2", "value2"));
    }
}