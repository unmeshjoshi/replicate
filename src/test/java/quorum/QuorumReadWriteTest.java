package quorum;

import common.TestUtils;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;
import distrib.patterns.quorum.QuorumKVStore;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class QuorumReadWriteTest {
    private int correlationId;

    @Test
    public void quorumReadWriteTest() throws IOException {

        List<QuorumKVStore> clusterNodes = startCluster(3);

        InetAddressAndPort primaryNodeAddress = clusterNodes.get(0).getClientConnectionAddress();
        SocketClient client = new SocketClient(primaryNodeAddress);

        RequestOrResponse requestOrResponse = createSetValueRequest("key", "value");
        RequestOrResponse setResponse = client.blockingSend(requestOrResponse);
        assertEquals("Success", new String(setResponse.getMessageBodyJson()));

        RequestOrResponse requestOrResponse1 = createGetValueRequest("key");
        RequestOrResponse getResponse = client.blockingSend(requestOrResponse1);
        assertEquals("value", new String(getResponse.getMessageBodyJson()));
    }

    private RequestOrResponse createGetValueRequest(String key) {
        GetValueRequest getValueRequest = new GetValueRequest(key);
        RequestOrResponse requestOrResponse1 = new RequestOrResponse(RequestId.GetValueRequest.getId(), JsonSerDes.serialize(getValueRequest), correlationId++);
        return requestOrResponse1;
    }

    private RequestOrResponse createSetValueRequest(String key, String value) {
        SetValueRequest setValueRequest = new SetValueRequest(key, value, "");
        RequestOrResponse requestOrResponse = new RequestOrResponse(RequestId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), 1);
        return requestOrResponse;
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
        QuorumKVStore primaryClusterNode = clusterNodes.get(0)
                ;
        RequestOrResponse setValueRequest = createSetValueRequest("key", "value");
        SocketClient client = new SocketClient(primaryClusterNode.getClientConnectionAddress());
        RequestOrResponse response = client.blockingSend(setValueRequest);
        assertEquals("Success", new String(response.getMessageBodyJson()));

        RequestOrResponse getValueRequest = createGetValueRequest("key");
        RequestOrResponse response1 = client.blockingSend(getValueRequest);

        assertEquals("value", new String(response1.getMessageBodyJson()));

        Config config = new Config(primaryClusterNode.getConfig().getWalDir().getAbsolutePath());
        InetAddressAndPort newClientAddress = TestUtils.randomLocalAddress();
        QuorumKVStore newInstance = new QuorumKVStore(new SystemClock(), config, newClientAddress, TestUtils.randomLocalAddress(), Arrays.asList(clusterNodes.get(1).getPeerConnectionAddress(), clusterNodes.get(2).getPeerConnectionAddress()));
        newInstance.start();
        assertEquals(2, newInstance.getGeneration());

        client = new SocketClient(newClientAddress);
        response = client.blockingSend(createSetValueRequest("key1", "value1"));

        assertEquals("Success", new String(response.getMessageBodyJson()));

        client = new SocketClient(primaryClusterNode.getClientConnectionAddress());
        setValueRequest = createSetValueRequest("key1", "value1");
        response = client.blockingSend(setValueRequest);
        assertEquals("Rejecting request from generation 1 as already accepted from generation 2", new String(response.getMessageBodyJson()));
    }
}