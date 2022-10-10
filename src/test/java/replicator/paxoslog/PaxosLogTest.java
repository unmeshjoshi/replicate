package replicator.paxoslog;

import replicator.common.ClusterTest;
import replicator.common.NetworkClient;
import replicator.common.TestUtils;
import distrib.patterns.common.*;
import replicator.paxos.messages.GetValueResponse;
import replicator.quorum.messages.GetValueRequest;
import replicator.twophasecommit.CompareAndSwap;
import replicator.twophasecommit.messages.ExecuteCommandRequest;
import replicator.twophasecommit.messages.ExecuteCommandResponse;
import replicator.wal.SetValueCommand;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class PaxosLogTest extends ClusterTest<PaxosLog> {
    @Before
    public void setUp() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peers) -> new PaxosLog(name, clock, config, clientConnectionAddress, peerConnectionAddress, peers));

    }

    @Test
    public void singleValuePaxosTest() throws IOException {
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand("title", "Microservices").serialize();
        var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command), nodes.get("athens").getClientConnectionAddress(), ExecuteCommandResponse.class);
        assertEquals(Optional.of("Microservices"), setValueResponse.getResponse());
    }

    @Test
    public void singleValueNullPaxosGetTest() throws IOException {
        var networkClient = new NetworkClient();
        var getValueResponse = networkClient.sendAndReceive(new GetValueRequest("title"), nodes.get("athens").getClientConnectionAddress(), GetValueResponse.class);
        assertEquals(Optional.empty(), getValueResponse.value);
    }

    @Test
    public void singleValuePaxosGetTest() throws IOException {
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand("title", "Microservices").serialize();
        var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command), nodes.get("athens").getClientConnectionAddress(), ExecuteCommandResponse.class);
        assertEquals(Optional.of("Microservices"), setValueResponse.getResponse());
        var getValueResponse = networkClient.sendAndReceive(new GetValueRequest("title"), nodes.get("athens").getClientConnectionAddress(), GetValueResponse.class);
        assertEquals(Optional.of("Microservices"), getValueResponse.value);
    }

    @Test
    public void executeMultipleCommands() throws IOException {
        var networkClient = new NetworkClient();
        PaxosLog athens = nodes.get("athens");
        var command = new SetValueCommand("title", "Microservices");
        var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command.serialize()), nodes.get("athens").getClientConnectionAddress(), ExecuteCommandResponse.class);
        assertEquals(Optional.of("Microservices"), setValueResponse.getResponse());

        PaxosLog byzantium = nodes.get("byzantium");
        command = new SetValueCommand("title2", "Distributed Systems");
        setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command.serialize()), nodes.get("athens").getClientConnectionAddress(), ExecuteCommandResponse.class);
        assertEquals(Optional.of("Distributed Systems"), setValueResponse.getResponse());

        assertEquals(2, nodes.get("athens").paxosLog.size());
        assertEquals(2, nodes.get("byzantium").paxosLog.size());
        assertEquals(2, nodes.get("cyrene").paxosLog.size());

        CompareAndSwap casCommand = new CompareAndSwap("title", Optional.empty(), "Microservices");
        var casResponse
                = networkClient.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);
        assertEquals(false, casResponse.isCommitted());
        assertEquals(Optional.of("Microservices"), casResponse.getResponse());

        assertEquals(3, nodes.get("athens").paxosLog.size());
        assertEquals(3, nodes.get("byzantium").paxosLog.size());
        assertEquals(3, nodes.get("cyrene").paxosLog.size());


        casCommand = new CompareAndSwap("title", Optional.of("Microservices"), "Event Driven Microservices");
        casResponse
                = networkClient.sendAndReceive(new ExecuteCommandRequest(casCommand.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);
        assertEquals(true, casResponse.isCommitted());
        assertEquals(Optional.of("Microservices"), casResponse.getResponse());

        assertEquals(4, nodes.get("athens").paxosLog.size());
        assertEquals(4, nodes.get("byzantium").paxosLog.size());
        assertEquals(4, nodes.get("cyrene").paxosLog.size());

        var getValueResponse = networkClient.sendAndReceive(new GetValueRequest("title"), athens.getClientConnectionAddress(), GetValueResponse.class);
        assertEquals(Optional.of("Event Driven Microservices"), getValueResponse.value);
    }
}