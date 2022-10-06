package distrib.patterns.paxoskv;

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

import static org.junit.Assert.*;

public class PaxosKVStoreTest extends ClusterTest<PaxosKVStore> {

    @Before
    public void setUp() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peers) -> new PaxosKVStore(name, clock, config, clientConnectionAddress, peerConnectionAddress, peers));
    }

    @Test
    public void singleValuePaxosTest() throws IOException {
        var client = new NetworkClient();
        var address = nodes.get("athens").getClientConnectionAddress();
        var response = client.sendAndReceive(new SetValueRequest("title", "Nicroservices"), address, SetValueResponse.class);
        assertEquals("Nicroservices", response.result);
    }

    @Test
    public void singleValueNullPaxosGetTest() throws IOException {
        var client = new NetworkClient();
        var address = nodes.get("athens").getClientConnectionAddress();
        var response = client.sendAndReceive(new GetValueRequest("title"), address, GetValueResponse.class);
        assertEquals(Optional.empty(), response.value);
    }

    @Test
    public void singleValuePaxosGetTest() throws IOException {
        var client = new NetworkClient();
        var address = nodes.get("athens").getClientConnectionAddress();
        var response = client.sendAndReceive(new SetValueRequest("title", "Nicroservices"), address, SetValueResponse.class);

        assertEquals("Nicroservices", response.result);

        var getResponse = client.sendAndReceive(new GetValueRequest("title"), address, GetValueResponse.class);
        assertEquals(Optional.of("Nicroservices"), getResponse.value);
    }
}