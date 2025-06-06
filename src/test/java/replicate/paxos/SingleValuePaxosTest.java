package replicate.paxos;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.paxos.messages.GetValueResponse;
import replicate.quorum.messages.GetValueRequest;
import replicate.quorum.messages.SetValueRequest;
import replicate.quorum.messages.SetValueResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Educational tests for Single Value Paxos demonstrating core consensus scenarios:
 * 1. Basic single value consensus
 * 2. Consensus with network partitions
 * 3. Conflict resolution between competing proposals
 * 4. Recovery from partial failures
 */
public class SingleValuePaxosTest extends ClusterTest<SingleValuePaxos> {
    
    // Educational naming: Ancient Greek city-states representing distributed nodes
    private SingleValuePaxos athens;    // Node 0: Often the primary proposer
    private SingleValuePaxos byzantium; // Node 1: Secondary node
    private SingleValuePaxos cyrene;    // Node 2: Third node for quorum

    @Before
    public void startPaxosCluster() throws IOException {
        super.nodes = TestUtils.startCluster(
            Arrays.asList("athens", "byzantium", "cyrene"),
            (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> {
                return new SingleValuePaxos(name, clock, config, clientConnectionAddress, 
                                          peerConnectionAddress, peerAddresses);
            }
        );
        athens = nodes.get("athens");
        byzantium = nodes.get("byzantium");
        cyrene = nodes.get("cyrene");
    }

    // ============================================================================
    // SCENARIO 1: Basic Single Value Consensus (Happy Path)
    // ============================================================================
    
    @Test
    public void demonstratesBasicConsensusOnSingleValue() throws IOException {
        // GIVEN: A cluster of 3 nodes (athens, byzantium, cyrene)
        // WHEN: Athens proposes "Microservices" as the value
        var response = proposeValue("title", "Microservices", athens);
        
        // THEN: Consensus is reached and the value is accepted
        assertTrue("Basic consensus should succeed with all nodes available", response.isSuccess());
        assertEquals("Microservices", response.getResult().result);
    }

    @Test
    public void handlesGetRequestOnEmptyState() throws IOException {
        // GIVEN: No value has been set in the cluster
        // WHEN: Client requests a value that doesn't exist
        var response = getValue("title", athens);
        
        // THEN: Should return empty rather than error
        assertEquals("Empty state should return Optional.empty", Optional.empty(), response.value);
    }

    // ============================================================================ 
    // SCENARIO 2: Network Partition and Conflict Resolution
    // This is the key educational scenario showing Paxos conflict resolution
    // ============================================================================
    
    @Test
    public void demonstratesConflictResolutionWithNetworkPartitions() throws IOException {
        verifyInitialServerConfiguration();
        
        // SCENARIO PHASE 1: Athens tries "Microservices" but network fails after prepare
        NetworkPartitionResult athensAttempt = simulatePartialNetworkFailure_AthensProposal();
        
        // SCENARIO PHASE 2: Byzantium tries "Distributed Systems" with different network failure  
        NetworkPartitionResult byzantiumAttempt = simulatePartialNetworkFailure_ByzantiumProposal();
        
        // SCENARIO PHASE 3: Cyrene resolves conflict by choosing highest generation value
        ConflictResolutionResult finalResolution = simulateConflictResolution_CyreneDecision();
        
        // FINAL VERIFICATION: All nodes converge on the same value
        verifyEventualConsistency("Distributed Systems");
    }

    // ============================================================================
    // Educational Helper Methods - Breaking down complex scenario into teachable steps
    // ============================================================================
    
    private void verifyInitialServerConfiguration() {
        // Educational assertion: Show students the server ID assignment
        int[] expectedServerIds = {0, 1, 2};
        Replica[] nodes = {athens, byzantium, cyrene};
        
        for (int i = 0; i < nodes.length; i++) {
            assertEquals("Server ID should match array index for educational clarity", 
                        expectedServerIds[i], nodes[i].getServerId());
        }
    }
    
    private NetworkPartitionResult simulatePartialNetworkFailure_AthensProposal() throws IOException {
        // EDUCATIONAL COMMENT: Athens can send prepare but not propose due to network partition
        athens.dropAfterNMessagesTo(byzantium, 1);  // Allow prepare, block propose  
        athens.dropAfterNMessagesTo(cyrene, 1);     // Allow prepare, block propose
        
        // Athens attempts to propose "Microservices"
        var response = proposeValue("title", "Microservices", athens);
        
        // EDUCATIONAL ASSERTION: Should fail because propose phase can't reach quorum
        Assert.assertTrue("Athens proposal should fail - can't reach quorum for propose phase", 
                         response.isError());
        
        // EDUCATIONAL STATE VERIFICATION: Check Paxos state after partial failure
        assertEquals("Athens should have higher promised generation from retry attempts",
                    new MonotonicId(2, 0), athens.paxosState.promisedGeneration());
        assertEquals("Athens should have accepted its own value despite network failure", 
                    Optional.of(new MonotonicId(1, 0)), athens.paxosState.acceptedGeneration());
        assertEquals("Athens should store the value it tried to propose",
                    "Microservices", athens.getAcceptedCommand().getValue());
        
        // Other nodes only saw the prepare phase, not the propose
        assertEquals("Byzantium saw prepare but not propose", 
                    new MonotonicId(1, 0), byzantium.paxosState.promisedGeneration());
        assertEquals("Byzantium has no accepted value", 
                    Optional.empty(), byzantium.paxosState.acceptedGeneration());
        assertEquals("Cyrene saw prepare but not propose",
                    new MonotonicId(1, 0), cyrene.paxosState.promisedGeneration());
        assertEquals("Cyrene has no accepted value",
                    Optional.empty(), cyrene.paxosState.acceptedGeneration());
        
        return new NetworkPartitionResult("Athens", "Microservices", false);
    }
    
    private NetworkPartitionResult simulatePartialNetworkFailure_ByzantiumProposal() throws IOException {
        // EDUCATIONAL COMMENT: Different network partition pattern for Byzantium
        byzantium.dropAfterNMessagesTo(cyrene, 1);  // Allow prepare, block propose to cyrene
        byzantium.dropMessagesTo(athens);           // Block all messages to athens
        
        var response = proposeValue("title", "Distributed Systems", byzantium);
        
        // EDUCATIONAL ASSERTION: Also fails but creates different Paxos state
        Assert.assertTrue("Byzantium proposal should also fail due to network partition", 
                         response.isError());
        
        // EDUCATIONAL STATE VERIFICATION: Byzantium's state after its attempt
        assertEquals("Byzantium should have higher promised generation", 
                    new MonotonicId(2, 1), byzantium.paxosState.promisedGeneration());
        assertEquals("Byzantium should have accepted its own value",
                    Optional.of(new MonotonicId(1, 1)), byzantium.paxosState.acceptedGeneration());
        assertEquals("Byzantium should store its proposed value",
                    "Distributed Systems", byzantium.getAcceptedCommand().getValue());
        
        // Cyrene saw byzantium's prepare but not propose
        assertEquals("Cyrene saw prepare from Byzantium (generation 1,1)",
                    new MonotonicId(1, 1), cyrene.paxosState.promisedGeneration());
        assertEquals("Cyrene still has no accepted value",
                    Optional.empty(), cyrene.paxosState.acceptedGeneration());
        
        // Athens state unchanged (messages were dropped)
        assertEquals("Athens state unchanged due to dropped messages",
                    new MonotonicId(2, 0), athens.paxosState.promisedGeneration());
        assertEquals("Athens still has its accepted value", 
                    Optional.of(new MonotonicId(1, 0)), athens.paxosState.acceptedGeneration());
        assertEquals("Athens still has its original value",
                    "Microservices", athens.getAcceptedCommand().getValue());
        
        return new NetworkPartitionResult("Byzantium", "Distributed Systems", false);
    }
    
    private ConflictResolutionResult simulateConflictResolution_CyreneDecision() throws IOException {
        // EDUCATIONAL COMMENT: Network heals, Cyrene must resolve conflicting values
        byzantium.reconnectTo(cyrene);
        cyrene.dropAfterNMessagesTo(byzantium, 2); // Allow prepare+response, block propose
        cyrene.dropMessagesTo(athens);             // Still partitioned from athens
        
        // Cyrene attempts its own value but must follow Paxos rules
        var response = proposeValue("title", "Event Driven Microservices", cyrene);
        
        // EDUCATIONAL ASSERTION: Fails to complete but resolves conflict correctly
        Assert.assertTrue("Cyrene's proposal should fail due to network issues", 
                         response.isError());
        
        // EDUCATIONAL KEY POINT: Cyrene chooses the highest generation value it learned about
        assertEquals("Cyrene should have highest promised generation",
                    new MonotonicId(2, 2), cyrene.paxosState.promisedGeneration());
        assertEquals("Cyrene should accept value from highest generation (Byzantium's)",
                    Optional.of(new MonotonicId(2, 2)), cyrene.paxosState.acceptedGeneration());
        
        // KEY EDUCATIONAL INSIGHT: Cyrene proposed its own value but accepted Byzantium's
        // This demonstrates Paxos conflict resolution - highest generation wins
        assertEquals("Cyrene should accept Byzantium's value, not its own proposal",
                    "Distributed Systems", cyrene.getAcceptedCommand().getValue());
        
        return new ConflictResolutionResult("Distributed Systems", "Event Driven Microservices");
    }
    
    private void verifyEventualConsistency(String expectedFinalValue) throws IOException {
        // EDUCATIONAL COMMENT: Network fully heals, all nodes should converge
        athens.reconnectTo(byzantium);
        byzantium.reconnectTo(athens);
        athens.reconnectTo(cyrene);
        
        // Query the final value - should trigger consistency resolution
        var getValueResponse = getValue("title", athens);
        assertEquals("All nodes should converge on the same final value",
                    Optional.of(expectedFinalValue), getValueResponse.value);
        
        // EDUCATIONAL VERIFICATION: All nodes now have the same value
        assertEquals("Athens should have final consistent value",
                    expectedFinalValue, athens.getAcceptedCommand().getValue());
        assertEquals("Byzantium should have final consistent value", 
                    expectedFinalValue, byzantium.getAcceptedCommand().getValue());
        assertEquals("Cyrene should have final consistent value",
                    expectedFinalValue, cyrene.getAcceptedCommand().getValue());
    }

    // ============================================================================
    // Educational Data Classes - Making test results more readable
    // ============================================================================
    
    private static class NetworkPartitionResult {
        final String proposer;
        final String proposedValue;
        final boolean succeeded;
        
        NetworkPartitionResult(String proposer, String proposedValue, boolean succeeded) {
            this.proposer = proposer;
            this.proposedValue = proposedValue;
            this.succeeded = succeeded;
        }
    }
    
    private static class ConflictResolutionResult {
        final String finalChosenValue;
        final String originalAttemptedValue;
        
        ConflictResolutionResult(String finalChosenValue, String originalAttemptedValue) {
            this.finalChosenValue = finalChosenValue;
            this.originalAttemptedValue = originalAttemptedValue;
        }
    }

    // ============================================================================
    // Utility Methods - Cleaner test code
    // ============================================================================
    
    private NetworkClient.Response<SetValueResponse> proposeValue(String key, String value, 
                                                                 SingleValuePaxos node) throws IOException {
        NetworkClient client = new NetworkClient();
        return client.sendAndReceive(new SetValueRequest(key, value), 
                                   node.getClientConnectionAddress(), 
                                   SetValueResponse.class);
    }
    
    private GetValueResponse getValue(String key, SingleValuePaxos node) throws IOException {
        var client = new NetworkClient();
        return client.sendAndReceive(new GetValueRequest(key), 
                                   node.getClientConnectionAddress(), 
                                   GetValueResponse.class).getResult();
    }
}