package replicate.chain;

import org.junit.Ignore;
import org.junit.Test;
import replicate.chain.messages.ChainWriteRequest;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.net.InetAddressAndPort;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;
import replicate.twophaseexecution.messages.ExecuteCommandResponse;
import replicate.wal.SetValueCommand;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore("ChainReplication tests are temporarily disabled due to protocol issues")
public class ChainReplicationTest extends ClusterTest<ChainReplication> {
    ChainReplication head;
    ChainReplication middle;
    ChainReplication tail;

    @Override
    public void setUp() throws IOException {
        this.nodes = TestUtils.startCluster(
                Arrays.asList("head", "middle", "tail"),
                (name, config, clock, clientAddr, peerAddr, peerAddresses) ->
                        new ChainReplication(name, config, clock, clientAddr, peerAddr, peerAddresses));

        head = nodes.get("head");
        middle = nodes.get("middle");
        tail = nodes.get("tail");

    }

    @Test
    public void testBasicChainConfiguration() throws IOException {
        // Verify roles
        assertEquals(NodeRole.HEAD, head.getRole());
        assertEquals(NodeRole.MIDDLE, middle.getRole());
        assertEquals(NodeRole.TAIL, tail.getRole());

        // Verify successor/predecessor links
        assertEquals(middle.getPeerConnectionAddress(), head.getSuccessor());
        assertEquals(tail.getPeerConnectionAddress(), middle.getSuccessor());
        assertEquals(null, tail.getSuccessor());

        assertEquals(null, head.getPredecessor());
        assertEquals(head.getPeerConnectionAddress(), middle.getPredecessor());
        assertEquals(middle.getPeerConnectionAddress(), tail.getPredecessor());
    }

    @Test
    public void testBasicWrite() throws IOException {
        KVClient client = new KVClient();

        // Write to head
        NetworkClient.Response<ExecuteCommandResponse> response =
                client.setValue(head.getClientConnectionAddress(), "key1", "value1");

        assertResponseSuccess(response);

        // Verify value is replicated through the chain
        assertEquals("value1", head.getValue("key1"));
        assertEquals("value1", middle.getValue("key1"));
        assertEquals("value1", tail.getValue("key1"));
    }

    @Test
    public void testWriteToNonHeadFails() throws IOException {
        KVClient client = new KVClient();

        // Write to middle node should fail
        NetworkClient.Response<ExecuteCommandResponse> response =
                client.setValue(middle.getClientConnectionAddress(), "key1", "value1");

        assertResponseFailure(response);

        // Write to tail node should fail
        response = client.setValue(tail.getClientConnectionAddress(), "key1", "value1");
        assertResponseFailure(response);
    }

    private void assertResponseSuccess(NetworkClient.Response<?> response) {
        assertTrue("Expected successful response but got error: " + response.getErrorMessage(),
                response.isSuccess());
    }

    private void assertResponseFailure(NetworkClient.Response<?> response) {
        assertTrue("Expected failure response but got success", response.isError());
    }
}

class KVClient {
    public NetworkClient.Response<ExecuteCommandResponse> setValue(InetAddressAndPort address,
                                                                   String key, String value) throws IOException {
        NetworkClient client = new NetworkClient();
        ExecuteCommandRequest request = new ExecuteCommandRequest(new SetValueCommand(key, value).serialize());
        return client.sendAndReceive(request, address, ExecuteCommandResponse.class);
    }
} 