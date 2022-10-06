package distrib.patterns.leaderbasedpaxoslog;

import common.ClusterTest;
import common.TestUtils;
import distrib.patterns.common.*;
import distrib.patterns.paxos.messages.GetValueResponse;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueRequest;
import distrib.patterns.quorum.messages.SetValueResponse;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class LeaderBasedPaxosLogTest extends ClusterTest<LeaderBasedPaxosLog> {
    @Before
    public void setUp() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peers) -> new LeaderBasedPaxosLog(name, clock, config, clientConnectionAddress, peerConnectionAddress, peers));

    }
    @Test
    public void singleValuePaxosTest() throws IOException {
        var athens = nodes.get("athens");

        athens.runElection();

        var networkClient = new NetworkClient();
        var setValueResponse = networkClient.sendAndReceive(new SetValueRequest("title", "Microservices"), nodes.get("athens").getClientConnectionAddress(), SetValueResponse.class);
        assertEquals("Microservices", setValueResponse.result);
    }

    @Test
    public void singleValueNullPaxosGetTest() throws IOException {
        var athens = nodes.get("athens");

        athens.runElection();

        var networkClient = new NetworkClient();
        var getValueResponse = networkClient.sendAndReceive(new GetValueRequest("title"), nodes.get("athens").getClientConnectionAddress(), GetValueResponse.class);
        assertEquals(Optional.empty(), getValueResponse.value);
    }

    @Test
    public void singleValuePaxosGetTest() throws IOException {
        var athens = nodes.get("athens");

        athens.runElection();

        var networkClient = new NetworkClient();
        var setValueResponse = networkClient.sendAndReceive(new SetValueRequest("title", "Microservices"), nodes.get("athens").getClientConnectionAddress(), SetValueResponse.class);
        assertEquals("Microservices", setValueResponse.result);

        var getValueResponse = networkClient.sendAndReceive(new GetValueRequest("title"), nodes.get("athens").getClientConnectionAddress(), GetValueResponse.class);
        assertEquals(Optional.of("Microservices"), getValueResponse.value);
    }
}