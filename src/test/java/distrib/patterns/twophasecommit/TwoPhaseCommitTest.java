package distrib.patterns.twophasecommit;

import common.TestUtils;
import distrib.patterns.common.NetworkClient;
import distrib.patterns.generationvoting.GenerationVoting;
import distrib.patterns.generationvoting.NextNumberRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TwoPhaseCommitTest {

    @Test
    public void commitsCommandInTwoPhases() throws IOException {
        List<TwoPhaseCommit> nodes = TestUtils.startCluster(3,
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
    }

}