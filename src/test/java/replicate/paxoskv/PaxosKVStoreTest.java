package replicate.paxoskv;

import org.junit.Before;
import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.paxos.messages.GetValueResponse;
import replicate.quorum.messages.GetValueRequest;
import replicate.quorum.messages.SetValueRequest;
import replicate.quorum.messages.SetValueResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

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
        var response = client.sendAndReceive(new SetValueRequest("title", "Nicroservices"), address, SetValueResponse.class).getResult();
        assertEquals("Nicroservices", response.result);
    }

    @Test
    public void singleValueNullPaxosGetTest() throws IOException {
        var client = new NetworkClient();
        var address = nodes.get("athens").getClientConnectionAddress();
        var response = client.sendAndReceive(new GetValueRequest("title"), address, GetValueResponse.class).getResult();
        assertEquals(Optional.of(""), response.value);
    }

    @Test
    public void singleValuePaxosGetTest() throws IOException {
        var client = new NetworkClient();
        var address = nodes.get("athens").getClientConnectionAddress();
        var response = client.sendAndReceive(new SetValueRequest("title", "Nicroservices"), address, SetValueResponse.class).getResult();

        assertEquals("Nicroservices", response.result);

        var getResponse = client.sendAndReceive(new GetValueRequest("title"), address, GetValueResponse.class).getResult();
        assertEquals(Optional.of("Nicroservices"), getResponse.value);
    }
}