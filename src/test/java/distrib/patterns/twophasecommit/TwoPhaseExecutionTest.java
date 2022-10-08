package distrib.patterns.twophasecommit;

import common.ClusterTest;
import common.TestUtils;
import distrib.patterns.common.NetworkClient;
import distrib.patterns.twophasecommit.messages.ExecuteCommandRequest;
import distrib.patterns.twophasecommit.messages.ExecuteCommandResponse;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.*;

public class TwoPhaseExecutionTest extends ClusterTest<TwoPhaseExecution> {

    //Incomplete requests do not create problems.
    @Test
    public void valuesAvailableOnlyIfQuorumHasAgreedToExecuteTheCommand() throws IOException {
        //created as instance variables, so that teardown can shutdown the cluster
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                    (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new TwoPhaseExecution(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));

        TwoPhaseExecution athens = nodes.get("athens");
        TwoPhaseExecution byzantium = nodes.get("byzantium");
        TwoPhaseExecution cyrene = nodes.get("cyrene");

        NetworkClient client = new NetworkClient();
        CompareAndSwap casCommand = new CompareAndSwap("title", Optional.empty(), "Microservices");
        ExecuteCommandResponse response
                = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);

        assertEquals(Optional.empty(), response.getResponse());
        assertTrue(response.isCommitted());

        assertEquals("Microservices", athens.getValue("title"));
        assertEquals("Microservices", byzantium.getValue("title"));
        assertEquals("Microservices", cyrene.getValue("title"));
    }

    @Test
    public void valuesUnavailableIfCommitMessagesAreLost() throws IOException {
       super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new TwoPhaseExecution(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        TwoPhaseExecution athens = nodes.get("athens");
        TwoPhaseExecution byzantium = nodes.get("byzantium");
        TwoPhaseExecution cyrene = nodes.get("cyrene");

        //commit messages to byzantium and cyrene are dropped.
        athens.dropMessagesToAfter(byzantium, 1);
        athens.dropMessagesToAfter(cyrene, 1);

        NetworkClient client = new NetworkClient();
        CompareAndSwap casCommand = new CompareAndSwap("title", Optional.empty(), "Microservices");
        ExecuteCommandResponse response
                = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);

        assertEquals(Optional.empty(), response.getResponse());
        assertTrue(response.isCommitted());

        assertEquals("Microservices", athens.getValue("title"));
        assertNull(byzantium.getValue("title"));
        assertNull(cyrene.getValue("title"));

    }

}