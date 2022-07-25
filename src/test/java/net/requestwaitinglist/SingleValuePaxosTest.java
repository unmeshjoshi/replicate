package net.requestwaitinglist;

import common.*;
import net.InetAddressAndPort;
import net.SocketClient;
import org.junit.Test;
import requests.GetValueRequest;
import requests.SetValueRequest;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SingleValuePaxosTest {

    @Test
    public void singleValuePaxosTest() throws IOException {
        SystemClock clock = new SystemClock();
        int clusterSize = 3;
        List<InetAddressAndPort> addresses = TestUtils.createNAddresses(clusterSize);
        List<InetAddressAndPort> clientInterfaceAddresses = TestUtils.createNAddresses(clusterSize);

        for (int i = 0; i < clusterSize; i++) {
            Config config = new Config(TestUtils.tempDir("SingleValuePaxosTest").getAbsolutePath());
            SingleValuePaxosClusterNode receivingClusterNode = new SingleValuePaxosClusterNode(clock, config, clientInterfaceAddresses.get(i), addresses.get(i), addresses);
            receivingClusterNode.start();
        }

        SetValueRequest setValueRequest = new SetValueRequest("key", "value", "");
        SocketClient client = new SocketClient(clientInterfaceAddresses.get(0));
        RequestOrResponse response = client.blockingSend(new RequestOrResponse(RequestId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), 1));

        assertEquals("value", JsonSerDes.deserialize(response.getMessageBodyJson(), String.class));
    }
}