package distrib.patterns.twophasecommit;

import common.ClusterTest;
import common.TestUtils;
import distrib.patterns.common.NetworkClient;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class TwoPhaseCommitTest extends ClusterTest<TwoPhaseCommit> {

    //Incomplete requests do not create problems.
    @Test
    public void valuesAvailableOnlyIfQuorumHasAgreedToExecuteTheCommand() throws IOException {
        //created as instance variables, so that teardown can shutdown the cluster
        super.nodes = TestUtils.startCluster(3,
                    (config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new TwoPhaseCommit(config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));

        TwoPhaseCommit athens = nodes.get(0);
        TwoPhaseCommit byzantium = nodes.get(1);
        TwoPhaseCommit cyrene = nodes.get(2);

        NetworkClient<ExecuteCommandResponse> client = new NetworkClient(ExecuteCommandResponse.class);
        CompareAndSwap casCommand = new CompareAndSwap("title", Optional.empty(), "Microservices");
        ExecuteCommandResponse response
                = client.send(new ExecuteCommandRequest(casCommand), athens.getClientConnectionAddress());

        assertEquals(Optional.empty(), response.getResponse());
        assertTrue(response.isCommitted());

        assertEquals("Microservices", athens.getValue("title"));
        assertEquals("Microservices", byzantium.getValue("title"));
        assertEquals("Microservices", cyrene.getValue("title"));
    }

    @Test
    public void valuesUnavailableIfCommitMessagesAreLost() throws IOException {
        List<TwoPhaseCommit> nodes = TestUtils.startCluster(3,
                (config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new TwoPhaseCommit(config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
        TwoPhaseCommit athens = nodes.get(0);
        TwoPhaseCommit byzantium = nodes.get(1);
        TwoPhaseCommit cyrene = nodes.get(2);

        //commit messages to byzantium and cyrene are dropped.
        athens.dropMessagesToAfter(byzantium, 1);
        athens.dropMessagesToAfter(cyrene, 1);

        NetworkClient<ExecuteCommandResponse> client = new NetworkClient(ExecuteCommandResponse.class);
        CompareAndSwap casCommand = new CompareAndSwap("title", Optional.empty(), "Microservices");
        ExecuteCommandResponse response
                = client.send(new ExecuteCommandRequest(casCommand), athens.getClientConnectionAddress());

        assertEquals(Optional.empty(), response.getResponse());
        assertTrue(response.isCommitted());

        assertEquals("Microservices", athens.getValue("title"));
        assertNull(byzantium.getValue("title"));
        assertNull(cyrene.getValue("title"));

    }

}