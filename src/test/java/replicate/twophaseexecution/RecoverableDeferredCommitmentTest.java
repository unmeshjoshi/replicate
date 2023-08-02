package replicate.twophaseexecution;

import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;
import replicate.twophaseexecution.messages.ExecuteCommandResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.*;

public class RecoverableDeferredCommitmentTest extends ClusterTest<RecoverableDeferredCommitment> {

    //Write Unit test for lost propose.

    @Test
    public void executesIncompleteCommits() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new RecoverableDeferredCommitment(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        DeferredCommitment athens = nodes.get("athens");
        DeferredCommitment byzantium = nodes.get("byzantium");
        DeferredCommitment cyrene = nodes.get("cyrene");

        //athens could send proposals (propose requests) to all the nodes.
        //athens --> propose ->athens (success)
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
        var firstResponse
                = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);

        assertFalse(firstResponse.isSuccess());

        athens.reconnectTo(byzantium);
        athens.reconnectTo(cyrene);

        casCommand = new CompareAndSwap("title", Optional.of("Microservices"), "Distributed Systems");
        var secondResponse
                = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);

        assertFalse(secondResponse.isSuccess());
        //the request executed only the incomplete command from the previous runs.


        assertEquals("Microservices", athens.getValue("title"));
        assertEquals("Microservices", byzantium.getValue("title"));
        assertEquals("Microservices", cyrene.getValue("title"));
    }
}