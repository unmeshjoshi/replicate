package replicate.generationvoting;

import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.generationvoting.messages.NextNumberRequest;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BallotVotingTest extends ClusterTest<BallotVoting> {

    @Test
    public void generateMonotonicNumbersWithQuorumVoting() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"), (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new BallotVoting(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        BallotVoting athens = nodes.get("athens");
        BallotVoting byzantium = nodes.get( "byzantium");
        BallotVoting cyrene = nodes.get("cyrene");

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
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene", "lamia", "marathon"), (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new BallotVoting(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        BallotVoting athens = nodes.get("athens");
        BallotVoting byzantium = nodes.get( "byzantium");
        BallotVoting cyrene = nodes.get("cyrene");
        BallotVoting lamia = nodes.get("lamia");
        BallotVoting marathon = nodes.get("marathon");

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
