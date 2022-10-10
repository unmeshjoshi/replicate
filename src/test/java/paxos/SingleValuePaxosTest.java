package paxos;

import common.ClusterTest;
import common.TestUtils;
import distrib.patterns.common.NetworkClient;
import distrib.patterns.paxos.SingleValuePaxos;
import distrib.patterns.paxos.messages.GetValueResponse;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueRequest;
import distrib.patterns.quorum.messages.SetValueResponse;
import org.junit.Before;
import org.junit.Ignore;
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

    @Ignore//TODO: Write a test for partial writes.
    public void singleValuePaxosGetTest() throws IOException {
        var client = new NetworkClient();

        //only athens has value Microservices
        //byzantium is empty, cyrene is empty
        athens.dropMessagesToAfter(byzantium, 1);
        athens.dropMessagesToAfter(cyrene, 1);

        var response = client.sendAndReceive(new SetValueRequest("title", "Microservices"), athens.getClientConnectionAddress(), SetValueResponse.class);

        assertEquals("Microservices", response.result);

        //only byzantium will have value Distributed Systems
        //athens has Microservices
        //cyrene is empty.
        byzantium.dropMessagesTo(athens);
        byzantium.dropMessagesToAfter(cyrene, 1);
        response = client.sendAndReceive(new SetValueRequest("title", "Distributed Systems"), byzantium.getClientConnectionAddress(), SetValueResponse.class);

        assertEquals("Microservices", response.result);

        //only cyrene will have value "Event Driven Microservices" 1
        //athens has Microservices 2
        //byzantium has Distributed Systems. 3
        cyrene.dropMessagesToAfter(byzantium, 1);
        response = client.sendAndReceive(new SetValueRequest("title", "Event Driven Microservices"), cyrene.getClientConnectionAddress(), SetValueResponse.class);

        assertEquals("Microservices", response.result);

        var getValueResponse = client.sendAndReceive(new GetValueRequest("title"), athens.getClientConnectionAddress(), GetValueResponse.class);

        assertEquals(Optional.of("Microservices"), getValueResponse.value);
    }

}