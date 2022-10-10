package replicator.generationvoting;

import replicator.common.ClusterTest;
import replicator.common.TestUtils;
import replicator.common.NetworkClient;
import replicator.generationvoting.messages.NextNumberRequest;
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

        NetworkClient client = new NetworkClient();
        Integer nextNumber = client.sendAndReceive(new NextNumberRequest(), athens.getClientConnectionAddress(), Integer.class);
        assertEquals(1, nextNumber.intValue());
        assertEquals(1, athens.generation);
        assertEquals(1, byzantium.generation);
        assertEquals(1, cyrene.generation);

        nextNumber = client.sendAndReceive(new NextNumberRequest(), athens.getClientConnectionAddress(), Integer.class);

        assertEquals(2, nextNumber.intValue());
        assertEquals(2, athens.generation);
        assertEquals(2, byzantium.generation);
        assertEquals(2, cyrene.generation);

        nextNumber = client.sendAndReceive(new NextNumberRequest(), athens.getClientConnectionAddress(), Integer.class);

        assertEquals(3, nextNumber.intValue());
        assertEquals(3, athens.generation);
        assertEquals(3, byzantium.generation);
        assertEquals(3, cyrene.generation);
    }

    @Test
    public void getsMonotonicNumbersWithFailures() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene", "lamia", "marathon"), (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new GenerationVoting(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        GenerationVoting athens = nodes.get("athens");
        GenerationVoting byzantium = nodes.get( "byzantium");
        GenerationVoting cyrene = nodes.get("cyrene");
        GenerationVoting lamia = nodes.get("lamia");
        GenerationVoting marathon = nodes.get("marathon");

        athens.dropMessagesTo(byzantium);

        NetworkClient client = new NetworkClient();
        Integer nextNumber = client.sendAndReceive(new NextNumberRequest(), athens.getClientConnectionAddress(), Integer.class);

        assertEquals(1, nextNumber.intValue());
        assertEquals(1, athens.generation);
        assertEquals(0, byzantium.generation);
        assertEquals(1, cyrene.generation);
        assertEquals(1, lamia.generation);
        assertEquals(1, marathon.generation);
    }

}
