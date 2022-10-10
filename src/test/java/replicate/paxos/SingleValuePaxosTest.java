package replicate.paxos;

import replicate.common.ClusterTest;
import replicate.common.TestUtils;
import replicate.common.MonotonicId;
import replicate.common.NetworkClient;
import replicate.net.InetAddressAndPort;
import replicate.paxos.messages.GetValueResponse;
import replicate.quorum.messages.GetValueRequest;
import replicate.quorum.messages.SetValueRequest;
import replicate.quorum.messages.SetValueResponse;
import org.junit.Assert;
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
        var response = setValue(new SetValueRequest("title", "Microservices"), athens.getClientConnectionAddress());

        Assert.assertEquals("Microservices", response.result);
    }

    @Test
    public void singleValueNullPaxosGetTest() throws IOException {
        var client = new NetworkClient();
        var response = client.sendAndReceive(new GetValueRequest("title"), athens.getClientConnectionAddress(), GetValueResponse.class);
        assertEquals(Optional.empty(), response.value);
    }

    @Test
    public void AllNodesChooseOneValueEvenWithIncompleteWrites() throws IOException {
        //only athens has value Microservices
        //byzantium is empty, cyrene is empty
        athens.dropAfterNMessagesTo(byzantium, 1);
        athens.dropAfterNMessagesTo(cyrene, 1);
        //prepare succeeds on athens, byzantium and cyrene.
        //propose succeeds only on athens
        var response = setValue(new SetValueRequest("title", "Microservices"), athens.getClientConnectionAddress());
        Assert.assertEquals("Error", response.result);

        assertEquals(athens.promisedGeneration, new MonotonicId(2, 1)); //prepare from second attempt
        assertEquals(athens.acceptedGeneration, Optional.of(new MonotonicId(1, 1)));
        assertEquals(athens.acceptedValue, Optional.of("Microservices"));

        //only byzantium will have value Distributed Systems
        //athens has Microservices
        //cyrene is empty.
        byzantium.dropAfterNMessagesTo(cyrene, 1);
        response = setValue(new SetValueRequest("title", "Distributed Systems"), byzantium.getClientConnectionAddress());

        Assert.assertEquals("Error", response.result);
        assertEquals(byzantium.promisedGeneration, new MonotonicId(2, 2)); //prepare from second attempt
        assertEquals(byzantium.acceptedGeneration,  Optional.of(new MonotonicId(1, 2)));
        assertEquals(byzantium.acceptedValue, Optional.of("Distributed Systems"));

        //only cyrene will have value "Event Driven Microservices" 1
        //athens has Microservices 2
        //byzantium has Distributed Systems. 3
        athens.reconnectTo(cyrene);
        byzantium.reconnectTo(cyrene);

        response = setValue(new SetValueRequest("title", "Event Driven Microservices"), cyrene.getClientConnectionAddress());

        Assert.assertEquals("Distributed Systems", response.result);
        assertEquals(cyrene.promisedGeneration, new MonotonicId(2, 3)); //prepare from second attempt
        assertEquals(cyrene.acceptedGeneration, Optional.of(new MonotonicId(2, 3)));
        assertEquals(cyrene.acceptedValue, Optional.of("Distributed Systems"));


//
//        var getValueResponse = client.sendAndReceive(new GetValueRequest("title"), athens.getClientConnectionAddress(), GetValueResponse.class);
//
//        assertEquals(Optional.of("Microservices"), getValueResponse.value);
    }

    private SetValueResponse setValue(SetValueRequest request, InetAddressAndPort clientConnectionAddress) {
        try {
            NetworkClient client = new NetworkClient();
            return client.sendAndReceive(request, clientConnectionAddress, SetValueResponse.class);
        } catch (Exception e) {
            return new SetValueResponse("Error");
        }
    }

}