package replicate.twophasecommit;

import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.twophasecommit.messages.ExecuteCommandRequest;
import replicate.twophasecommit.messages.ExecuteCommandResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class NonBlockingTwoPhaseExecutionTest extends ClusterTest<NonBlockingTwoPhaseExecution> {

    @Test
    public void executesIncompleteCommits() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new NonBlockingTwoPhaseExecution(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        TwoPhaseExecution athens = nodes.get("athens");
        TwoPhaseExecution byzantium = nodes.get("byzantium");
        TwoPhaseExecution cyrene = nodes.get("cyrene");

        //athens could send proposals (propose requests) to all the nodes.
        //athens --> propose ->byzantium (success)
        //athens --> commit -> byzantium (fails) after first message
        //which means it was fine to commit and execute
        //but it failed to communicate at this point.
        //commit messages to byzantium and cyrene are dropped.
        //byzantium and cyrene do not know what to do.
        athens.dropAfterNMessagesTo(byzantium, 1);
        athens.dropAfterNMessagesTo(cyrene, 1);

        NetworkClient client = new NetworkClient();
        CompareAndSwap casCommand = new CompareAndSwap("title", Optional.empty(), "Microservices");
        ExecuteCommandResponse response
                = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);


        athens.reconnectTo(byzantium);
        athens.reconnectTo(cyrene);

        casCommand = new CompareAndSwap("title", Optional.of("Microservices"), "Distributed Systems");
        response
                = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);

        assertEquals("Microservices", athens.getValue("title"));
        assertEquals("Microservices", byzantium.getValue("title"));
        assertEquals("Microservices", cyrene.getValue("title"));
    }
}