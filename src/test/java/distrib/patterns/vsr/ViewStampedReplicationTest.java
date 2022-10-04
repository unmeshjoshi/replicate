package distrib.patterns.vsr;

import common.ClusterTest;
import common.TestUtils;
import distrib.patterns.common.NetworkClient;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.twophasecommit.ExecuteCommandRequest;
import distrib.patterns.twophasecommit.ExecuteCommandResponse;
import distrib.patterns.wal.SetValueCommand;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ViewStampedReplicationTest extends ClusterTest<ViewStampedReplication> {

    @Test
    public void testNormalOperationWithoutFailures() throws IOException, InterruptedException {

        super.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                        (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new ViewStampedReplication(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));

        ViewStampedReplication athens = nodes.get("athens");
        ViewStampedReplication byzantium = nodes.get("byzantium");
        ViewStampedReplication cyrene = nodes.get("cyrene");

        InetAddressAndPort primaryAddress = athens.getPrimaryAddress();

        NetworkClient<ExecuteCommandResponse> client = new NetworkClient(ExecuteCommandResponse.class);
        SetValueCommand casCommand = new SetValueCommand("title", "Microservices");
        ExecuteCommandResponse response
                = client.send(new ExecuteCommandRequest(casCommand.serialize()), primaryAddress);

        assertEquals(Optional.of("Microservices"), response.getResponse());
    }

    @Test
    public void changesViewAndElectsNewPrimary() throws IOException, InterruptedException {
        super.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                        (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new ViewStampedReplication(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));

        ViewStampedReplication athens = nodes.get("athens");

        InetAddressAndPort primaryAddress = athens.getPrimaryAddress();
        ViewStampedReplication primary = getPrimaryNode(primaryAddress);

        NetworkClient<ExecuteCommandResponse> client = new NetworkClient(ExecuteCommandResponse.class);
        SetValueCommand casCommand = new SetValueCommand("title", "Microservices");
        ExecuteCommandResponse response
                = client.send(new ExecuteCommandRequest(casCommand.serialize()), primaryAddress);
        assertEquals(Optional.of("Microservices"), response.getResponse());

        List<ViewStampedReplication> backUpNodes = getBackUpNodes(primaryAddress);
        assertEquals(backUpNodes.get(0).getPrimaryAddress(), primaryAddress);
        assertEquals(backUpNodes.get(1).getPrimaryAddress(), primaryAddress);

        System.out.println("Shutting down " + primary.getName());
        primary.shutdown();

        TestUtils.waitUntilTrue(()->{
            return !backUpNodes.get(0).getPrimaryAddress().equals(primaryAddress) && !backUpNodes.get(1).getPrimaryAddress().equals(primaryAddress)
                    && backUpNodes.get(0).getViewNumber() == 1 && backUpNodes.get(1).getViewNumber() == 1;
        }, "Waiting for new primary to be elected", Duration.ofSeconds(5));
    }

    private List<ViewStampedReplication> getBackUpNodes(InetAddressAndPort primaryAddress) {
        return this.nodes.values().stream().filter(n -> !n.getPeerConnectionAddress().equals(primaryAddress)).collect(Collectors.toList());
    }

    private ViewStampedReplication getPrimaryNode(InetAddressAndPort primaryAddress) {
        return this.nodes.values().stream().filter(n -> n.getPeerConnectionAddress().equals(primaryAddress)).findFirst().get();
    }

}