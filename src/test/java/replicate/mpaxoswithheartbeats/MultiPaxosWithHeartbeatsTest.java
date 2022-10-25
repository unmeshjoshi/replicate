package replicate.mpaxoswithheartbeats;

import org.junit.Before;
import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.paxos.messages.GetValueResponse;
import replicate.quorum.messages.GetValueRequest;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;
import replicate.twophaseexecution.messages.ExecuteCommandResponse;
import replicate.wal.SetValueCommand;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class MultiPaxosWithHeartbeatsTest extends ClusterTest<MultiPaxosWithHeartbeats> {
    MultiPaxosWithHeartbeats leader = null;
    List<MultiPaxosWithHeartbeats> followers = null;

    @Before
    public void setUp() throws IOException {
        super.nodes = TestUtils.startCluster( Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peers) -> new MultiPaxosWithHeartbeats(name, clock, config, clientConnectionAddress, peerConnectionAddress, peers));

        TestUtils.waitUntilTrue(()->{
            return nodes.entrySet().stream().anyMatch(e -> e.getValue().role == ServerRole.Leader);
        }, "Waiting for leader election", Duration.ofSeconds(10));

        TestUtils.waitUntilTrue(()->{
            return nodes.entrySet().stream().filter(e -> e.getValue().role == ServerRole.Follower).count() == (nodes.size() - 1);
        }, "Waiting for all other nodes to become follower", Duration.ofSeconds(10));

        leader = getLeader();
        followers = getFollowers();
    }

    private List<MultiPaxosWithHeartbeats> getFollowers() {
        return nodes.entrySet().stream().filter(e -> !e.getValue().isLeader()).map(e -> e.getValue()).collect(Collectors.toList());
    }

    private MultiPaxosWithHeartbeats getLeader() {
        return nodes.entrySet().stream().filter(e -> e.getValue().isLeader()).findFirst().get().getValue();
    }

    @Test
    public void setsSingleValue() throws Exception {
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand("title", "Microservices").serialize();
        var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command), leader.getClientConnectionAddress(), ExecuteCommandResponse.class);
        assertEquals(Optional.of("Microservices"), setValueResponse.getResponse());
    }

    @Test
    public void singleValueNullPaxosGetTest() throws Exception {
        var networkClient = new NetworkClient();
        var getValueResponse = networkClient.sendAndReceive(new GetValueRequest("title"), leader.getClientConnectionAddress(), GetValueResponse.class);
        assertEquals(Optional.empty(), getValueResponse.value);
    }

    @Test
    public void singleValuePaxosGetTest() throws Exception {
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand("title", "Microservices").serialize();
        var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command), leader.getClientConnectionAddress(), ExecuteCommandResponse.class);
        var getValueResponse = networkClient.sendAndReceive(new GetValueRequest("title"), leader.getClientConnectionAddress(), GetValueResponse.class);
        assertEquals(Optional.of("Microservices"), getValueResponse.value);
    }


    @Test
    public void leaderElectionCompletesIncompletePaxosRuns() throws Exception {
        MultiPaxosWithHeartbeats follower1 = followers.get(0);
        MultiPaxosWithHeartbeats follower2 = followers.get(1);

        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand("title", "Microservices").serialize();
        var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command), leader.getClientConnectionAddress(), ExecuteCommandResponse.class);

        leader.dropMessagesTo(follower1); //propose messages fail
        leader.dropMessagesTo(follower2); //propose messages fail

        try {
            command = new SetValueCommand("author", "Martin").serialize();
            setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command), leader.getClientConnectionAddress(), ExecuteCommandResponse.class);
            fail("Expected to fail because athens will be unable to reach quorum");
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(2, leader.paxosLog.size()); //uncommitted second entry
        assertEquals(1, follower1.paxosLog.size()); //only first entry.
        assertEquals(1, follower2.paxosLog.size()); //only first entry.

        assertTrue(leader.paxosLog.get(0).committedValue().isPresent());
        assertTrue(follower1.paxosLog.get(0).committedValue().isPresent());
        assertTrue(follower2.paxosLog.get(0).committedValue().isPresent());

        assertFalse(leader.paxosLog.get(1).committedValue().isPresent());

        assertNull(leader.getValue("author"));

        //election which is equivalent to prepare phase of basic paxos, checks
        //and completes pending log entries from majority quorum of the servers.
        TestUtils.waitUntilTrue(()->{
            return follower1.isLeader() || follower2.isLeader();
        }, "Waiting for leader election", Duration.ofSeconds(5));


        var oldLeader = leader;
        var newLeader = getLeader();
        assertEquals(2, oldLeader.paxosLog.size());
        assertEquals(1, newLeader.paxosLog.size());
//
//        assertEquals("Martin", leader.getValue("author"));
//        assertEquals("Martin", follower1.getValue("author"));
//        assertEquals("Martin", follower2.getValue("author"));
    }
}