package distrib.patterns.leaderbasedpaxoslog;

import common.TestUtils;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;
import distrib.patterns.quorum.messages.GetValueRequest;
import distrib.patterns.quorum.messages.SetValueRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LeaderBasedPaxosLogClusterNodeTest {

    @Test
    public void singleValuePaxosTest() throws IOException {
        List<LeaderBasedPaxosLogClusterNode> clusterNodes = startCluster(3);

        LeaderBasedPaxosLogClusterNode athens = clusterNodes.get(0);
        LeaderBasedPaxosLogClusterNode byzantium = clusterNodes.get(1);
        LeaderBasedPaxosLogClusterNode cyrene = clusterNodes.get(2);

        athens.runElection();

        RequestOrResponse requestOrResponse = createSetValueRequest("key", "value");

        SocketClient client = new SocketClient(athens.getClientConnectionAddress());
        RequestOrResponse response = client.blockingSend(requestOrResponse);

        assertEquals("Success", JsonSerDes.deserialize(response.getMessageBodyJson(), String.class));
    }

    @Test
    public void singleValueNullPaxosGetTest() throws IOException {

        List<LeaderBasedPaxosLogClusterNode> clusterNodes = startCluster(3);

        LeaderBasedPaxosLogClusterNode athens = clusterNodes.get(0);
        LeaderBasedPaxosLogClusterNode byzantium = clusterNodes.get(1);
        LeaderBasedPaxosLogClusterNode cyrene = clusterNodes.get(2);

        athens.runElection();
        RequestOrResponse setValueRequest = createGetValueRequest("key");
        SocketClient client = new SocketClient(athens.getClientConnectionAddress());
        RequestOrResponse response = client.blockingSend(setValueRequest);

        assertEquals(null, JsonSerDes.deserialize(response.getMessageBodyJson(), String.class));
    }

    @Test
    public void singleValuePaxosGetTest() throws IOException {

        List<LeaderBasedPaxosLogClusterNode> clusterNodes = startCluster(3);

        LeaderBasedPaxosLogClusterNode athens = clusterNodes.get(0);
        LeaderBasedPaxosLogClusterNode byzantium = clusterNodes.get(1);
        LeaderBasedPaxosLogClusterNode cyrene = clusterNodes.get(2);

        athens.runElection();

        RequestOrResponse requestOrResponse = createSetValueRequest("key", "value");
        SocketClient client = new SocketClient(athens.getClientConnectionAddress());
        RequestOrResponse response = client.blockingSend(requestOrResponse);

        assertEquals("Success", JsonSerDes.deserialize(response.getMessageBodyJson(), String.class));

        RequestOrResponse getValueRequest = createGetValueRequest("key");
        RequestOrResponse response1 = client.blockingSend(getValueRequest);

        assertEquals("value", JsonSerDes.deserialize(response1.getMessageBodyJson(), String.class));
    }


    private RequestOrResponse createSetValueRequest(String key, String value) {
        SetValueRequest setValueRequest = new SetValueRequest(key, value);
        RequestOrResponse requestOrResponse = new RequestOrResponse(RequestId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), 1);
        return requestOrResponse;
    }

    private RequestOrResponse createGetValueRequest(String key) {
        GetValueRequest setValueRequest = new GetValueRequest(key);
        RequestOrResponse requestOrResponse = new RequestOrResponse(RequestId.GetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), 1);
        return requestOrResponse;
    }


    private List<LeaderBasedPaxosLogClusterNode> startCluster(int clusterSize) throws IOException {
        SystemClock clock = new SystemClock();
        List<InetAddressAndPort> addresses = TestUtils.createNAddresses(clusterSize);
        List<InetAddressAndPort> clientInterfaceAddresses = TestUtils.createNAddresses(clusterSize);

        List<LeaderBasedPaxosLogClusterNode> clusterNodes = new ArrayList<>();
        for (int i = 0; i < clusterSize; i++) {
            Config config = new Config(TestUtils.tempDir("LeaderBasedPaxosLogTest").getAbsolutePath());
            LeaderBasedPaxosLogClusterNode paxosKVClusterNode = new LeaderBasedPaxosLogClusterNode(clock, config, clientInterfaceAddresses.get(i), addresses.get(i), addresses);
            clusterNodes.add(paxosKVClusterNode);

            paxosKVClusterNode.start();
        }
        return clusterNodes;
    }

}