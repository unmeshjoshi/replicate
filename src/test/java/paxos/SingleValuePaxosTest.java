package paxos;

import common.ClusterTest;
import common.TestUtils;
import distrib.patterns.common.*;
import distrib.patterns.paxos.GetValueResponse;
import distrib.patterns.paxos.SingleValuePaxos;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueRequest;
import distrib.patterns.quorum.messages.SetValueResponse;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class SingleValuePaxosTest extends ClusterTest<SingleValuePaxos> {
    SingleValuePaxos athens;
    SingleValuePaxos byzantium;
    SingleValuePaxos cyrene;

    @Before
    public void startCluster() throws IOException {
        AtomicInteger id = new AtomicInteger(1);
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> {
                        config.setServerId(id.getAndIncrement());
                        return new SingleValuePaxos(name, clock, config, clientConnectionAddress, peerConnectionAddress, peerAddresses);
                });
        athens = nodes.get("athens");
        byzantium = nodes.get("byzantium");
        cyrene = nodes.get("cyrene");
    }

    @Test
    public void singleValuePaxosTest() throws IOException {
        var client = new NetworkClient();
        var response = client.sendAndReceive(new SetValueRequest("title", "Microservices"), athens.getClientConnectionAddress(), SetValueResponse.class);

        assertEquals("Microservices", response.result);
    }

    @Test
    public void singleValueNullPaxosGetTest() throws IOException {
        var client = new NetworkClient();
        var response = client.sendAndReceive(new GetValueRequest("title"), athens.getClientConnectionAddress(), GetValueResponse.class);

        assertEquals(Optional.empty(), response.value);
    }

    @Test
    public void singleValuePaxosGetTest() throws IOException {
        var client = new NetworkClient();
        var response = client.sendAndReceive(new SetValueRequest("title", "Microservices"), athens.getClientConnectionAddress(), SetValueResponse.class);

        assertEquals("Microservices", response.result);

        var getValueResponse = client.sendAndReceive(new GetValueRequest("title"), athens.getClientConnectionAddress(), GetValueResponse.class);

        assertEquals(Optional.of("Microservices"), getValueResponse.value);
    }

}