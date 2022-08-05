package paxos;

import common.TestUtils;
import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;
import distrib.patterns.paxos.SingleValuePaxosClusterNode;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SingleValuePaxosTest {

    @Test
    public void singleValuePaxosTest() throws IOException {
        List<InetAddressAndPort> clientInterfaceAddresses = startCluster(3);

        RequestOrResponse requestOrResponse = createSetValueRequest("key", "value");

        SocketClient client = new SocketClient(clientInterfaceAddresses.get(0));
        RequestOrResponse response = client.blockingSend(requestOrResponse);

        assertEquals("value", JsonSerDes.deserialize(response.getMessageBodyJson(), String.class));
    }

    @Test
    public void singleValueNullPaxosGetTest() throws IOException {
        List<InetAddressAndPort> clientInterfaceAddresses = startCluster(3);

        RequestOrResponse setValueRequest = createGetValueRequest("key");
        SocketClient client = new SocketClient(clientInterfaceAddresses.get(0));
        RequestOrResponse response = client.blockingSend(setValueRequest);

        assertEquals(null, JsonSerDes.deserialize(response.getMessageBodyJson(), String.class));
    }

    @Test
    public void singleValuePaxosGetTest() throws IOException {
        List<InetAddressAndPort> clientInterfaceAddresses = startCluster(3);

        RequestOrResponse requestOrResponse = createSetValueRequest("key", "value");
        SocketClient client = new SocketClient(clientInterfaceAddresses.get(0));
        RequestOrResponse response = client.blockingSend(requestOrResponse);

        assertEquals("value", JsonSerDes.deserialize(response.getMessageBodyJson(), String.class));

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


    private List<InetAddressAndPort> startCluster(int clusterSize) throws IOException {
        SystemClock clock = new SystemClock();
        List<InetAddressAndPort> addresses = TestUtils.createNAddresses(clusterSize);
        List<InetAddressAndPort> clientInterfaceAddresses = TestUtils.createNAddresses(clusterSize);

        for (int i = 0; i < clusterSize; i++) {
            Config config = new Config(TestUtils.tempDir("SingleValuePaxosTest").getAbsolutePath());
            SingleValuePaxosClusterNode receivingClusterNode = new SingleValuePaxosClusterNode(clock, config, clientInterfaceAddresses.get(i), addresses.get(i), addresses);
            receivingClusterNode.start();
        }
        return clientInterfaceAddresses;
    }

}