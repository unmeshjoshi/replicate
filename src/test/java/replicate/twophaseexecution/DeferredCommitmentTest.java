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

public class DeferredCommitmentTest extends ClusterTest<DeferredCommitment> {

    //Assignment: Write a test for incomleteWritesAreNotAvailableInReads.
    @Test
    public void incomleteWritesAreNotAvailableInReads() throws IOException {
        //created as instance variables, so that teardown can shutdown the cluster
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new DeferredCommitment(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));

        DeferredCommitment athens = nodes.get("athens");
        DeferredCommitment byzantium = nodes.get("byzantium");
        DeferredCommitment cyrene = nodes.get("cyrene");

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        NetworkClient client = new NetworkClient();
        CompareAndSwap casCommand = new CompareAndSwap("title", Optional.empty(), "Microservices");
        NetworkClient.Response<ExecuteCommandResponse> response = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);

        assertTrue(response.isError());
        assertEquals(response.getErrorMessage(), Optional.of("Request expired"));

        assertNull(athens.getValue("title"));
        assertNull(byzantium.getValue("title"));
        assertNull(cyrene.getValue("title"));
    }

    @Test
    public void valuesAvailableOnlyIfQuorumHasAgreedToExecuteTheCommand() throws IOException {
        //created as instance variables, so that teardown can shutdown the cluster
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                    (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new DeferredCommitment(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));

        DeferredCommitment athens = nodes.get("athens");
        DeferredCommitment byzantium = nodes.get("byzantium");
        DeferredCommitment cyrene = nodes.get("cyrene");

        NetworkClient client = new NetworkClient();
        //register a lease with ttl.
        CompareAndSwap casCommand = new CompareAndSwap("title", Optional.empty(), "Microservices");
        ExecuteCommandResponse response
                = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class).getResult();

        assertEquals(Optional.empty(), response.getResponse());
        assertTrue(response.isCommitted());

        assertEquals("Microservices", athens.getValue("title"));
        assertEquals("Microservices", byzantium.getValue("title"));
        assertEquals("Microservices", cyrene.getValue("title"));
    }

    @Test
    public void valuesUnavailableIfCommitMessagesAreLost() throws IOException {
       super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new DeferredCommitment(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        DeferredCommitment athens = nodes.get("athens");
        DeferredCommitment byzantium = nodes.get("byzantium");
        DeferredCommitment cyrene = nodes.get("cyrene");

        //commit messages to byzantium and cyrene are dropped.
        athens.dropAfterNMessagesTo(byzantium, 1);
        athens.dropAfterNMessagesTo(cyrene, 1);

        NetworkClient client = new NetworkClient();
        CompareAndSwap casCommand = new CompareAndSwap("title", Optional.empty(), "Microservices");
        ExecuteCommandResponse response
                = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class).getResult();

        assertEquals(Optional.empty(), response.getResponse());
        assertTrue(response.isCommitted());

        assertEquals("Microservices", athens.getValue("title"));
        assertNull(byzantium.getValue("title"));
        assertNull(cyrene.getValue("title"));

    }

}