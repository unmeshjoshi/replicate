package distrib.patterns.generationvoting;

import common.ClusterTest;
import common.TestUtils;
import distrib.patterns.common.NetworkClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class GenerationVotingTest extends ClusterTest<GenerationVoting> {

    @Test
    public void generateMonotonicNumbersWithQuorumVoting() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"), (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new GenerationVoting(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        GenerationVoting athens = nodes.get("athens");
        GenerationVoting byzantium = nodes.get( "byzantium");
        GenerationVoting cyrene = nodes.get("cyrene");

        NetworkClient<Integer> client = new NetworkClient(Integer.class);
        Integer nextNumber = client.send(new NextNumberRequest(), athens.getClientConnectionAddress());

        assertEquals(1, nextNumber.intValue());
        assertEquals(1, athens.generation);
        assertEquals(1, byzantium.generation);
        assertEquals(1, cyrene.generation);

    }

}