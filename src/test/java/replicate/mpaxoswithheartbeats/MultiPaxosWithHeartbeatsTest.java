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
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class MultiPaxosWithHeartbeatsTest extends ClusterTest<MultiPaxosWithHeartbeats> {
    MultiPaxosWithHeartbeats leader = null;
    List<MultiPaxosWithHeartbeats> followers = null;

    @Before
    public void setUp() throws IOException {
        super.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peers) -> new MultiPaxosWithHeartbeats(name, clock, config, clientConnectionAddress, peerConnectionAddress, peers));

        TestUtils.waitUntilTrue(() -> {
            return nodes.entrySet().stream().anyMatch(e -> e.getValue().role == ServerRole.Leader);
        }, "Waiting for leader election", Duration.ofSeconds(10));

        TestUtils.waitUntilTrue(() -> {
            return nodes.entrySet().stream().filter(e -> e.getValue().role == ServerRole.Follower).count() == (nodes.size() - 1);
        }, "Waiting for all other nodes to become follower", Duration.ofSeconds(10));

        leader = getLeader();
        followers = getFollowers();
    }

    private List<MultiPaxosWithHeartbeats> getFollowers() {
        return nodes.entrySet().stream().filter(e -> e.getValue().isFollower()).map(e -> e.getValue()).collect(Collectors.toList());
    }

    private MultiPaxosWithHeartbeats getLeader() {
        return getLeaderFrom(nodes.values());
    }

    private MultiPaxosWithHeartbeats getLeaderFrom(Collection<MultiPaxosWithHeartbeats> nodes) {
        return nodes.stream().filter(e -> e.isLeader()).findFirst().get();
    }

    @Test
    public void setsSingleValue() throws Exception {
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand("title", "Microservices").serialize();
        var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command), leader.getClientConnectionAddress(), ExecuteCommandResponse.class).getResult();
        assertEquals(Optional.of("Microservices"), setValueResponse.getResponse());
    }

    @Test
    public void singleValueNullPaxosGetTest() throws Exception {
        var networkClient = new NetworkClient();
        var getValueResponse = networkClient.sendAndReceive(new GetValueRequest("title"), leader.getClientConnectionAddress(), GetValueResponse.class).getResult();
        assertEquals(Optional.empty(), getValueResponse.value);
    }

    @Test
    public void singleValuePaxosGetTest() throws Exception {
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand("title", "Microservices").serialize();
        var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command), leader.getClientConnectionAddress(), ExecuteCommandResponse.class);
        var getValueResponse = networkClient.sendAndReceive(new GetValueRequest("title"), leader.getClientConnectionAddress(), GetValueResponse.class).getResult();
        assertEquals(Optional.of("Microservices"), getValueResponse.value);
    }


    @Test //FIXME:Flaky test
    public void oldLeaderStepsDownWhenHeartbeatForLowerBallotIsRejected() throws Exception {
        MultiPaxosWithHeartbeats follower1 = followers.get(0);
        MultiPaxosWithHeartbeats follower2 = followers.get(1);

        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand("title", "Microservices").serialize();
        var setValueResponse = networkClient.sendAndReceive(new ExecuteCommandRequest(command), leader.getClientConnectionAddress(), ExecuteCommandResponse.class);

        System.out.println("leader = " + leader.getName());
        System.out.println("followers = " + followers.stream().map(f -> f.getName()).collect(Collectors.toList()));

        leader.dropMessagesTo(follower1); //both way failure.
        follower1.dropMessagesTo(leader);
        leader.dropMessagesTo(follower2);
        follower2.dropMessagesTo(leader);

        command = new SetValueCommand("author", "Martin").serialize();
        var response = networkClient.sendAndReceive(new ExecuteCommandRequest(command), leader.getClientConnectionAddress(), ExecuteCommandResponse.class);
        assertTrue("Expected to fail because athens will be unable to reach quorum", response.isError());

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
        TestUtils.waitUntilTrue(() -> {
            return follower1.isLeader() || follower2.isLeader();
        }, "Waiting for leader election", Duration.ofSeconds(5));


        var oldLeader = leader;
        var newLeader = getLeaderFrom(Arrays.asList(follower1, follower2));

        System.out.println("oldLeader = " + oldLeader.getName());
        assertEquals("Old leader should still thinks that its the leader", oldLeader.role, ServerRole.Leader);
        assertTrue("Old leader should still thinks that its the leader", oldLeader.isLeader());
        assertTrue("New leader should have higher ballot than old leader", newLeader.promisedGeneration.isAfter(oldLeader.promisedGeneration));

        oldLeader.reconnectTo(follower1);
        follower1.reconnectTo(oldLeader);

        TestUtils.waitUntilTrue(() -> {
            return oldLeader.isFollower();
        }, "Old leader should step down", Duration.ofSeconds(2));

        assertEquals(newLeader.promisedGeneration, oldLeader.promisedGeneration);
    }
}