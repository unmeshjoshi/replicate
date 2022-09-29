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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class SingleValuePaxosTest extends ClusterTest<SingleValuePaxos> {

    private int clusterSize = 3;

    SingleValuePaxos athens;
    SingleValuePaxos byzantium;
    SingleValuePaxos cyrene;

    @Before
    public void startCluster() throws IOException {
        AtomicInteger id = new AtomicInteger(1);
        super.nodes = TestUtils.startCluster(clusterSize,
                (config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> {
                        config.setServerId(id.getAndIncrement());
                        return new SingleValuePaxos(clock, config, clientConnectionAddress, peerConnectionAddress, peerAddresses);
                });
        athens = nodes.get(0);
        byzantium = nodes.get(1);
        cyrene = nodes.get(2);
    }

    @Test
    public void singleValuePaxosTest() throws IOException {
        NetworkClient<SetValueResponse> client = new NetworkClient<>(SetValueResponse.class);
        SetValueResponse response = client.send(createSetValueRequest("title", "Microservices"), athens.getClientConnectionAddress());

        assertEquals("Microservices", response.getResult());
    }

    @Test
    public void singleValueNullPaxosGetTest() throws IOException {
        NetworkClient<GetValueResponse> client2 = new NetworkClient<>(GetValueResponse.class);
        GetValueRequest getValueRequest = new GetValueRequest("title");
        GetValueResponse response1 = client2.send(getValueRequest, athens.getClientConnectionAddress());

        assertEquals(Optional.empty(), response1.getValue());
    }

    @Test
    public void singleValuePaxosGetTest() throws IOException {
        NetworkClient<SetValueResponse> client = new NetworkClient<>(SetValueResponse.class);
        SetValueResponse response = client.send(createSetValueRequest("title", "Microservices"), athens.getClientConnectionAddress());

        assertEquals("Microservices", response.getResult());

        NetworkClient<GetValueResponse> client2 = new NetworkClient<>(GetValueResponse.class);
        GetValueRequest getValueRequest = new GetValueRequest("title");
        GetValueResponse response1 = client2.send(getValueRequest, athens.getClientConnectionAddress());

        assertEquals(Optional.of("Microservices"), response1.getValue());
    }

    private SetValueRequest createSetValueRequest(String key, String value) {
        SetValueRequest setValueRequest = new SetValueRequest(key, value);
        return setValueRequest;
    }

}