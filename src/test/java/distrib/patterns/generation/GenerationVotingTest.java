package distrib.patterns.generation;

import common.TestUtils;
import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.RequestId;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GenerationVotingTest {

    private static class NetworkClient<T> {
        Class<T> responseClass;

        public NetworkClient(Class<T> responseClass) {
            this.responseClass = responseClass;
        }

        public T send(Request request, InetAddressAndPort address) throws IOException {
            SocketClient<Object> client = new SocketClient<>(address);
            RequestOrResponse getResponse = client.blockingSend(new RequestOrResponse(request.getRequestId().getId(),
                        JsonSerDes.serialize(request)));
            return JsonSerDes.deserialize(getResponse.getMessageBodyJson(), responseClass);
        }
    }

    @Test
    public void generateMonotonicNumbersWithQuorumVoting() throws IOException {
        List<GenerationVoting> nodes = TestUtils.startCluster(3, (config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new GenerationVoting(config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        GenerationVoting athens = nodes.get(0);
        GenerationVoting byzantium = nodes.get(1);
        GenerationVoting cyrene = nodes.get(2);

        NetworkClient<Integer> client = new NetworkClient(Integer.class);
        Integer nextNumber = client.send(new NextNumberRequest(), athens.getClientConnectionAddress());

        assertEquals(1, nextNumber.intValue());
        assertEquals(1, athens.generation);
        assertEquals(1, byzantium.generation);
        assertEquals(1, cyrene.generation);

    }

}