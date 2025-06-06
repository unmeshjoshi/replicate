package replicate.multipaxos;

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

import static org.junit.Assert.*;

/**
 * Educational tests for Multi-Paxos demonstrating distributed log consensus:
 * 1. Leader election and single value consensus
 * 2. Multiple values in replicated log
 * 3. Leader failure and recovery scenarios
 * 4. Log completion after partial failures
 */
public class MultiPaxosTest extends ClusterTest<MultiPaxos> {
    
    // Educational naming: Greek city-states for MultiPaxos cluster
    private MultiPaxos athens;
    private MultiPaxos byzantium; 
    private MultiPaxos cyrene;
    
    @Before
    public void setupMultiPaxosCluster() throws IOException {
        super.nodes = TestUtils.startCluster(
            cityStateNames("athens", "byzantium", "cyrene"),
            (name, config, clock, clientConnectionAddress, peerConnectionAddress, peers) -> 
                new MultiPaxos(name, clock, config, clientConnectionAddress, peerConnectionAddress, peers)
        );
        
        // Cache references for educational clarity
        athens = nodes.get("athens");
        byzantium = nodes.get("byzantium");
        cyrene = nodes.get("cyrene");
    }

    // ============================================================================
    // SCENARIO 1: Basic Leader Election and Single Value Consensus
    // ============================================================================
    
    @Test
    public void demonstratesLeaderElectionAndBasicConsensus() throws Exception {
        // GIVEN: A MultiPaxos cluster with no leader
        // WHEN: Athens runs leader election
        electLeader(athens, "Athens should become leader");
        
        // THEN: Athens can accept and replicate a single value
        String result = executeCommand("title", "Microservices", athens);
        assertEquals("Basic consensus through elected leader should work", 
                    "Microservices", result);
    }

    @Test
    public void handlesEmptyLogQueries() throws Exception {
        // GIVEN: An elected leader with empty log
        electLeader(athens, "Need leader for queries");
        
        // WHEN: Client queries non-existent key
        // THEN: Should return empty, not error
        String result = queryValue("title", athens);
        assertNull("Empty log should return null for non-existent keys", result);
    }

    // ============================================================================
    // SCENARIO 2: Multiple Commands in Replicated Log  
    // ============================================================================
    
    @Test
    public void demonstratesSequentialCommandReplication() throws Exception {
        // GIVEN: Athens is the elected leader
        electLeader(athens, "Athens leads the cluster");
        
        // WHEN: Multiple commands are executed in sequence
        executeCommand("title", "Microservices", athens);
        String result = queryValue("title", athens);
        assertEquals("First command should be replicated", "Microservices", result);
        
        // THEN: Later queries should return the stored value
        String laterResult = queryValue("title", athens);
        assertEquals("Value should persist in replicated log", "Microservices", laterResult);
    }

    // ============================================================================
    // SCENARIO 3: Leader Failure and Log Recovery (The Key Educational Scenario)
    // This demonstrates the critical Multi-Paxos recovery mechanism
    // ============================================================================
    
    @Test 
    public void demonstratesLeaderFailureRecoveryAndLogCompletion() throws Exception {
        // PHASE 1: Establish Athens as leader and execute first command successfully
        LeadershipPhase phase1 = establishInitialLeadership();
        
        // PHASE 2: Simulate network partition during second command
        PartialFailureScenario phase2 = simulatePartialCommandFailure();
        
        // PHASE 3: New leader election and automatic log completion
        LogRecoveryResult phase3 = demonstrateAutomaticLogRecovery();
        
        // FINAL VERIFICATION: All nodes have consistent, complete log
        verifyLogConsistencyAcrossCluster();
    }
    
    // ============================================================================
    // Educational Helper Methods - Breaking down complex Multi-Paxos scenarios
    // ============================================================================
    
    private LeadershipPhase establishInitialLeadership() throws Exception {
        // EDUCATIONAL STEP 1: Athens becomes leader
        electLeader(athens, "Athens establishes initial leadership");
        
        // EDUCATIONAL STEP 2: Successfully replicate first command
        String firstResult = executeCommand("title", "Microservices", athens);
        assertEquals("First command should succeed under stable leadership", 
                    "Microservices", firstResult);
        
        // EDUCATIONAL VERIFICATION: All nodes have the first entry
        verifyLogSizeAcrossCluster(1, "After first successful command");
        verifyAllNodesHaveCommittedEntry(0, "First entry should be committed everywhere");
        
        return new LeadershipPhase("Athens", "Microservices", true);
    }
    
    private PartialFailureScenario simulatePartialCommandFailure() throws Exception {
        // EDUCATIONAL SETUP: Create network partition that prevents full replication
        athens.dropMessagesTo(byzantium); // Athens can't reach Byzantium
        athens.dropMessagesTo(cyrene);    // Athens can't reach Cyrene
        
        // EDUCATIONAL ATTEMPT: Try to execute second command (should fail)
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand("author", "Martin").serialize();
        var response = networkClient.sendAndReceive(
            new ExecuteCommandRequest(command), 
            athens.getClientConnectionAddress(), 
            ExecuteCommandResponse.class
        );
        
        // EDUCATIONAL ASSERTION: Command should fail due to inability to reach quorum
        assertTrue("Command should fail - Athens cannot reach quorum due to network partition", 
                  response.isError());
        
        // EDUCATIONAL STATE VERIFICATION: Check log states after partial failure
        assertEquals("Athens should have 2 entries (1 committed, 1 pending)", 
                    2, athens.paxosLog.size());
        assertEquals("Byzantium should only have 1 entry (the committed one)", 
                    1, byzantium.paxosLog.size());
        assertEquals("Cyrene should only have 1 entry (the committed one)", 
                    1, cyrene.paxosLog.size());
        
        // EDUCATIONAL KEY INSIGHT: First entry is committed everywhere, second is only on Athens
        verifyAllNodesHaveCommittedEntry(0, "First entry remains committed despite network issues");
        assertFalse("Athens' second entry should be uncommitted due to network failure",
                   athens.paxosLog.get(1).committedValue().isPresent());
        
        // EDUCATIONAL VERIFICATION: The failed command value is not accessible
        assertNull("Failed command should not be queryable", athens.getValue("author"));
        
        return new PartialFailureScenario("author", "Martin", false);
    }
    
    private LogRecoveryResult demonstrateAutomaticLogRecovery() throws Exception {
        // EDUCATIONAL SETUP: Network heals, Byzantium becomes new leader
        athens.reconnectTo(byzantium);
        athens.reconnectTo(cyrene);
        
        // EDUCATIONAL KEY POINT: New leader election triggers log completion
        electLeader(byzantium, "Byzantium becomes new leader and will complete incomplete log entries");
        
        // EDUCATIONAL VERIFICATION: Log completion should happen automatically
        verifyLogSizeAcrossCluster(2, "New leader should complete all pending log entries");
        
        // EDUCATIONAL KEY INSIGHT: The previously failed command is now successful
        assertEquals("Previously failed command should now be accessible after log completion",
                    "Martin", athens.getValue("author"));
        assertEquals("All nodes should have the recovered value", 
                    "Martin", byzantium.getValue("author"));
        assertEquals("Cyrene should also have the recovered value",
                    "Martin", cyrene.getValue("author"));
        
        return new LogRecoveryResult("Martin", 2, true);
    }
    
    private void verifyLogConsistencyAcrossCluster() {
        // EDUCATIONAL FINAL CHECK: All nodes should have identical logs
        assertEquals("All nodes should have same log size after recovery", 
                    athens.paxosLog.size(), byzantium.paxosLog.size());
        assertEquals("Athens and Cyrene should have same log size", 
                    athens.paxosLog.size(), cyrene.paxosLog.size());
        
        // EDUCATIONAL VERIFICATION: All values should be accessible from any node
        assertEquals("Title should be accessible from any node", 
                    "Microservices", athens.getValue("title"));
        assertEquals("Author should be accessible from any node", 
                    "Martin", byzantium.getValue("author"));
    }

    // ============================================================================
    // Educational Utility Methods - Making tests more readable and teachable
    // ============================================================================
    
    private void electLeader(MultiPaxos node, String educationalContext) throws Exception {
        node.leaderElection();
        TestUtils.waitUntilTrue(() -> node.isLeader(), 
                               "Waiting for leader election: " + educationalContext, 
                               Duration.ofSeconds(2));
        assertTrue(educationalContext + " - Node should be leader after election", 
                  node.isLeader());
    }
    
    private String executeCommand(String key, String value, MultiPaxos leader) throws Exception {
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand(key, value).serialize();
        var response = networkClient.sendAndReceive(
            new ExecuteCommandRequest(command), 
            leader.getClientConnectionAddress(), 
            ExecuteCommandResponse.class
        ).getResult();
        
        return response.getResponse().orElse(null);
    }
    
    private String queryValue(String key, MultiPaxos node) throws Exception {
        var networkClient = new NetworkClient();
        var response = networkClient.sendAndReceive(
            new GetValueRequest(key), 
            node.getClientConnectionAddress(), 
            GetValueResponse.class
        ).getResult();
        
        return response.value.orElse(null);
    }
    
    private void verifyLogSizeAcrossCluster(int expectedSize, String context) {
        assertEquals(context + " - Athens log size", expectedSize, athens.paxosLog.size());
        assertEquals(context + " - Byzantium log size", expectedSize, byzantium.paxosLog.size());
        assertEquals(context + " - Cyrene log size", expectedSize, cyrene.paxosLog.size());
    }
    
    private void verifyAllNodesHaveCommittedEntry(int index, String context) {
        assertTrue(context + " - Athens entry " + index + " should be committed",
                  athens.paxosLog.get(index).committedValue().isPresent());
        assertTrue(context + " - Byzantium entry " + index + " should be committed",
                  byzantium.paxosLog.get(index).committedValue().isPresent());
        assertTrue(context + " - Cyrene entry " + index + " should be committed",
                  cyrene.paxosLog.get(index).committedValue().isPresent());
    }

    // ============================================================================
    // Educational Data Classes - Making test results more structured and readable
    // ============================================================================
    
    private static class LeadershipPhase {
        final String leader;
        final String firstValue;
        final boolean successful;
        
        LeadershipPhase(String leader, String firstValue, boolean successful) {
            this.leader = leader;
            this.firstValue = firstValue;
            this.successful = successful;
        }
    }
    
    private static class PartialFailureScenario {
        final String attemptedKey;
        final String attemptedValue;
        final boolean succeeded;
        
        PartialFailureScenario(String attemptedKey, String attemptedValue, boolean succeeded) {
            this.attemptedKey = attemptedKey;
            this.attemptedValue = attemptedValue;
            this.succeeded = succeeded;
        }
    }
    
    private static class LogRecoveryResult {
        final String recoveredValue;
        final int finalLogSize;
        final boolean recoverySuccessful;
        
        LogRecoveryResult(String recoveredValue, int finalLogSize, boolean recoverySuccessful) {
            this.recoveredValue = recoveredValue;
            this.finalLogSize = finalLogSize;
            this.recoverySuccessful = recoverySuccessful;
        }
    }
    
    private static List<String> cityStateNames(String... names) {
        return Arrays.asList(names);
    }
}