package distrib.patterns.quorumconsensus;

import common.TestUtils;
import distrib.patterns.common.Config;
import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.Node;
import distrib.patterns.common.SystemClock;
import distrib.patterns.net.InetAddressAndPort;
import org.junit.Test;
import quorum.KVClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class QuorumConsensusKVStoreTest {

    @Test
    public void setsVersionWithKey() throws IOException {
        List<Node> kvStores = startCluster(3);
        Node athens = kvStores.get(0);
        Node byzantium = kvStores.get(1);
        Node cyrene = kvStores.get(2);

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


    private List<Node> startCluster(int clusterSize) throws IOException {
        List<Node> clusterNodes = new ArrayList<>();
        SystemClock clock = new SystemClock();
        List<InetAddressAndPort> addresses = TestUtils.createNAddresses(clusterSize);
        List<InetAddressAndPort> clientInterfaceAddresses = TestUtils.createNAddresses(clusterSize);

        for (int i = 0; i < clusterSize; i++) {
            Config config = new Config(TestUtils.tempDir("clusternode_" + i).getAbsolutePath());
            Node receivingClusterNode = new Node(config, clientInterfaceAddresses.get(i), addresses.get(i), addresses, new QuorumKV(clock, true));
            receivingClusterNode.start();
            clusterNodes.add(receivingClusterNode);
        }
        return clusterNodes;
    }


}