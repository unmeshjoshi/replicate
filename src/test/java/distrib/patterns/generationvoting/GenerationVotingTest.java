package distrib.patterns.generationvoting;

import common.TestUtils;
import distrib.patterns.common.NetworkClient;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GenerationVotingTest {

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