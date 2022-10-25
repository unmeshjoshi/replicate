package replicate.paxoslog;

import org.junit.Before;
import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.paxos.messages.GetValueResponse;
import replicate.quorum.messages.GetValueRequest;
import replicate.twophaseexecution.CompareAndSwap;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;
import replicate.twophaseexecution.messages.ExecuteCommandResponse;
import replicate.wal.SetValueCommand;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.*;

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
        setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command.serialize()), byzantium.getClientConnectionAddress(), ExecuteCommandResponse.class);
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


    @Test
    public void selectsNextIndexIfOtherValueIsSelectedForIndexInPraxosPrepare() throws IOException {
        var networkClient = new NetworkClient();
        PaxosLog athens = nodes.get("athens");
        PaxosLog byzantium = nodes.get("byzantium");
        PaxosLog cyrene = nodes.get("cyrene");

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);
        var command = new SetValueCommand("title", "Microservices");
        try {
            var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);
            fail("Expect an exception, as quorum communication fails after multiple attempts");
        } catch (Exception e) {

        }

        byzantium.dropAfterNMessagesTo(cyrene, 2);

        try {
            command = new SetValueCommand("title", "Distributed Systems");
            var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command.serialize()), byzantium.getClientConnectionAddress(), ExecuteCommandResponse.class);
            fail("Expect an exception, as quorum communication fails after multiple attempts");
        } catch (Exception e) {

        }
        assertEquals(1, athens.paxosLog.size());
        assertEquals(1, byzantium.paxosLog.size());
        assertEquals(1, cyrene.paxosLog.size());


        try {
            command = new SetValueCommand("title", "Event Driven Microservices");
            var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command.serialize()), cyrene.getClientConnectionAddress(), ExecuteCommandResponse.class);
            fail("Expect an exception, as quorum communication fails after multiple attempts");
        } catch (Exception e) {
        }
        assertEquals(1, athens.paxosLog.size());
        assertEquals(1, byzantium.paxosLog.size());
        assertEquals(1, cyrene.paxosLog.size());


        byzantium.reconnectTo(cyrene);
        try {
            command = new SetValueCommand("title", "Event Driven Microservices");
            var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command.serialize()), cyrene.getClientConnectionAddress(), ExecuteCommandResponse.class);
            assertEquals(setValueResponse.getResponse(), Optional.of("Event Driven Microservices"));
        } catch (Exception e) {
            fail("Should be able to commit");
        }

        assertEquals(2, athens.paxosLog.size());
        assertEquals(2, byzantium.paxosLog.size());
        assertEquals(2, cyrene.paxosLog.size());

        assertEquals("Event Driven Microservices", athens.getValue("title"));
        assertEquals("Event Driven Microservices", byzantium.getValue("title"));
        assertEquals("Event Driven Microservices", cyrene.getValue("title"));

    }


    @Test
    public void triesNextLogIndexOnlyAfterCommittingValueAtCurrentIndex() throws IOException, InterruptedException {
        var networkClient = new NetworkClient();
        PaxosLog athens = nodes.get("athens");
        PaxosLog byzantium = nodes.get("byzantium");
        PaxosLog cyrene = nodes.get("cyrene");

        athens.dropAfterNMessagesTo(byzantium, 1); //prepare succeeds, propose fails. So propose succeeds only on athens.
        athens.dropMessagesTo(cyrene);
        try {
            var command = new SetValueCommand("title", "Microservices");
            var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command.serialize()), athens.getClientConnectionAddress(), ExecuteCommandResponse.class);
            fail("Except an exception, as quorum communication fails after multiple attempts");
        } catch (Exception e) {

        }
        assertEquals(1, athens.paxosLog.size());
        assertEquals(1, byzantium.paxosLog.size());
        assertEquals(0, cyrene.paxosLog.size());

        athens.reconnectTo(cyrene);

        var command = new SetValueCommand("newTitle", "Event Driven Microservices");
        ExecuteCommandRequest request = new ExecuteCommandRequest(command.serialize());
        var setValueResponse = networkClient.sendAndReceive(request, cyrene.getClientConnectionAddress(), ExecuteCommandResponse.class);
        assertEquals(Optional.of("Event Driven Microservices"), setValueResponse.getResponse());

        assertEquals(2, byzantium.paxosLog.size());
        assertEquals(2, cyrene.paxosLog.size());
        assertEquals(2, athens.paxosLog.size());

    }

}