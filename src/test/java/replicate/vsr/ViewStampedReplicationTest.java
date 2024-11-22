package replicate.vsr;

import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.net.InetAddressAndPort;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;
import replicate.twophaseexecution.messages.ExecuteCommandResponse;
import replicate.wal.SetValueCommand;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ViewStampedReplicationTest extends ClusterTest<ViewStampedReplication> {

//    @Test
//    public void commitedEntriesCanHaveDifferentViewNumberOnDifferentServers() throws IOException, InterruptedException {
//        super.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
//                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new ViewStampedReplication(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));
//
//        var athens = nodes.get("athens");
//        var byzantium = nodes.get("byzantium");
//        var cyrene = nodes.get("cyrene");
//
//        athens.
//
//
//    }

    @Test
    public void testNormalOperationWithoutFailures() throws IOException, InterruptedException {
        super.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                        (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new ViewStampedReplication(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));

        var athens = nodes.get("athens");
        var byzantium = nodes.get("byzantium");
        var cyrene = nodes.get("cyrene");

        var primaryAddress = getPrimaryNode(athens.getPrimaryAddress()).getClientConnectionAddress();

        var client = new NetworkClient();
        var casCommand = new SetValueCommand("title", "Microservices");
        var response
                = client.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), primaryAddress, ExecuteCommandResponse.class).getResult();

        assertEquals(Optional.of("Microservices"), response.getResponse());
    }

    @Test
    public void changesViewAndElectsNewPrimary() throws IOException, InterruptedException {
        super.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                        (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new ViewStampedReplication(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));

        var athens = nodes.get("athens");

        var primary = getPrimaryNode(athens.getPrimaryAddress());
        var primaryAddressClientConnectionAddress = primary.getClientConnectionAddress();
        var primaryPeerAddress = primary.getPeerConnectionAddress();

        var client = new NetworkClient();
        var setValueCommand = new SetValueCommand("title", "Microservices");
        var response
                = client.sendAndReceive(new ExecuteCommandRequest(setValueCommand.serialize()), primaryAddressClientConnectionAddress, ExecuteCommandResponse.class).getResult();
        assertEquals(Optional.of("Microservices"), response.getResponse());

        List<ViewStampedReplication> backUpNodes = getBackUpNodes(primaryPeerAddress);
        assertEquals(backUpNodes.get(0).getPrimaryAddress(), primaryPeerAddress);
        assertEquals(backUpNodes.get(1).getPrimaryAddress(), primaryPeerAddress);

        System.out.println("Shutting down " + primary.getName());
        primary.shutdown();

        TestUtils.waitUntilTrue(()->{
            return !backUpNodes.get(0).getPrimaryAddress().equals(primaryPeerAddress) && !backUpNodes.get(1).getPrimaryAddress().equals(primaryPeerAddress)
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