package net.requestwaitinglist;

import common.Config;
import common.JsonSerDes;
import common.RequestOrResponse;
import common.TestUtils;
import requests.GetValueRequest;
import requests.SetValueRequest;
import common.RequestId;
import net.ClientConnection;
import net.InetAddressAndPort;
import net.SocketClient;
import common.SystemClock;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class QuorumReadWriteTest {
    @Test
    public void asyncMessagePassingAcrossServers() throws IOException {
        SystemClock clock = new SystemClock();

        int clusterSize = 3;
        List<InetAddressAndPort> addresses = TestUtils.createNAddresses(clusterSize);
        List<InetAddressAndPort> clientInterfaceAddresses = TestUtils.createNAddresses(clusterSize);

        for (int i = 0; i < clusterSize; i++) {
            Config config = new Config(TestUtils.tempDir("AsyncMessagePassingTest").getAbsolutePath());
            SingleValueClusterNode receivingClusterNode = new SingleValueClusterNode(clock, config, clientInterfaceAddresses.get(i), addresses.get(i), addresses);
            receivingClusterNode.start();
        }

        SetValueRequest setValueRequest = new SetValueRequest("key", "value", "");
        SocketClient client = new SocketClient(clientInterfaceAddresses.get(0));
        RequestOrResponse response = client.blockingSend(new RequestOrResponse(RequestId.SetValueRequest.getId(),
        JsonSerDes.serialize(setValueRequest), 1));

        assertEquals("Success", new String(response.getMessageBodyJson()));

        GetValueRequest getValueRequest = new GetValueRequest();
        RequestOrResponse response1 = client.blockingSend(
                new RequestOrResponse(RequestId.GetValueRequest.getId(), JsonSerDes.serialize(getValueRequest), 2));

        assertEquals("value", new String(response1.getMessageBodyJson()));
    }


    @Test
    public void quorumHandlerTest() {
        CompletableFuture f = new CompletableFuture();
        f.whenComplete((r,e)->{
            System.out.println("r = " + r);
            System.out.println("e = " + e);
        });

        f.complete(true);

        AtomicBoolean responseReceived = new AtomicBoolean(false);

        RequestOrResponse request = new RequestOrResponse(RequestId.SetValueRequest.getId(), JsonSerDes.serialize(new SetValueRequest("key", "value", "")), 1);
        SingleValueClusterNode.WriteQuorumCallback handler = new SingleValueClusterNode.WriteQuorumCallback(1,request, new ClientConnection() {
            @Override
            public void write(RequestOrResponse response) {
                responseReceived.set(true);
            }

            @Override
            public void close() {

            }
        });


        handler.onResponse(new RequestOrResponse(RequestId.GetValueRequest.getId(), "Success".getBytes(), 1));
        TestUtils.waitUntilTrue(()->responseReceived.get(), "Future should complete on response", Duration.ofMillis(100));

    }

}