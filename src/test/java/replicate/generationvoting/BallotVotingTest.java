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
        Integer nextNumber = client.sendAndReceive(new NextNumberRequest(), athens.getClientConnectionAddress(), Integer.class).getResult();
        assertEquals(1, nextNumber.intValue());
        assertEquals(1, athens.ballot);
        assertEquals(1, byzantium.ballot);
        assertEquals(1, cyrene.ballot);

        nextNumber = client.sendAndReceive(new NextNumberRequest(), athens.getClientConnectionAddress(), Integer.class).getResult();

        assertEquals(2, nextNumber.intValue());
        assertEquals(2, athens.ballot);
        assertEquals(2, byzantium.ballot);
        assertEquals(2, cyrene.ballot);

        nextNumber = client.sendAndReceive(new NextNumberRequest(), athens.getClientConnectionAddress(), Integer.class).getResult();

        assertEquals(3, nextNumber.intValue());
        assertEquals(3, athens.ballot);
        assertEquals(3, byzantium.ballot);
        assertEquals(3, cyrene.ballot);
    }

    @Test
    public void getsMonotonicNumbersWithFailures() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene", "delphi", "ephesus"), (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new BallotVoting(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        BallotVoting athens = nodes.get("athens");
        BallotVoting byzantium = nodes.get( "byzantium");
        BallotVoting cyrene = nodes.get("cyrene");
        BallotVoting delphi = nodes.get("delphi");
        BallotVoting ephesus = nodes.get("ephesus");

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(ephesus);

        NetworkClient client = new NetworkClient();
        Integer firstNumber = client.sendAndReceive(new NextNumberRequest(), athens.getClientConnectionAddress(), Integer.class).getResult();

        assertEquals(1, firstNumber.intValue());
        assertEquals(1, athens.ballot);
        assertEquals(0, byzantium.ballot);
        assertEquals(1, cyrene.ballot);
        assertEquals(1, delphi.ballot);
        assertEquals(0, ephesus.ballot);


        ephesus.dropMessagesTo(athens);
        ephesus.dropMessagesTo(cyrene);

        Integer secondNumber = client.sendAndReceive(new NextNumberRequest(), ephesus.getClientConnectionAddress(), Integer.class).getResult();


        assertEquals(2, secondNumber.intValue());
        assertEquals(1, athens.ballot);
        assertEquals(2, byzantium.ballot);
        assertEquals(1, cyrene.ballot);
        assertEquals(2, delphi.ballot);
        assertEquals(2, ephesus.ballot);


        Integer thirdNumber = client.sendAndReceive(new NextNumberRequest(), athens.getClientConnectionAddress(), Integer.class).getResult();
        assertEquals(3, thirdNumber.intValue());
        //try generating more numbers connecting to different nodes.

        Integer fourthNumber = client.sendAndReceive(new NextNumberRequest(), ephesus.getClientConnectionAddress(), Integer.class).getResult();
        assertEquals(4, fourthNumber.intValue());

    }

}
