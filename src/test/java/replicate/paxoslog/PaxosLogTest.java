package replicate.paxoslog;

import org.junit.Before;
import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.paxos.PaxosState;
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

/**
 * Educational tests for PaxosLog demonstrating distributed log with automatic consensus:
 * 1. Basic single command execution and querying
 * 2. Multiple commands building a replicated log
 * 3. Compare-and-swap operations for atomic updates
 * 4. Log reconciliation after network partitions
 * 5. Automatic index selection and conflict resolution
 * 
 * Key Educational Concepts:
 * - Each command gets automatically assigned to next available log index
 * - Network partitions create incomplete log entries that need reconciliation
 * - Multiple nodes attempting different values at same index triggers conflict resolution
 * - Log entries must be committed in order (no gaps allowed)
 */
public class PaxosLogTest extends ClusterTest<PaxosLog> {
    
    // Educational naming: Greek city-states for PaxosLog cluster
    private PaxosLog athens;
    private PaxosLog byzantium;
    private PaxosLog cyrene;
    
    @Before
    public void setupPaxosLogCluster() throws IOException {
        super.nodes = TestUtils.startCluster(
            Arrays.asList("athens", "byzantium", "cyrene"),
            (name, config, clock, clientConnectionAddress, peerConnectionAddress, peers) -> 
                new PaxosLog(name, clock, config, clientConnectionAddress, peerConnectionAddress, peers)
        );
        
        // Cache references for educational clarity
        athens = nodes.get("athens");
        byzantium = nodes.get("byzantium");
        cyrene = nodes.get("cyrene");
    }

    // ============================================================================
    // SCENARIO 1: Basic Single Command Execution (Happy Path)
    // ============================================================================
    
    @Test
    public void demonstratesBasicCommandExecution() throws IOException {
        // GIVEN: An empty PaxosLog cluster
        // WHEN: A single SetValue command is executed
        String result = executeSetValueCommand("title", "Microservices", athens);
        
        // THEN: The command should succeed and be queryable
        assertEquals("Basic command execution should work", "Microservices", result);
    }

    @Test
    public void handlesQueryOnEmptyLog() throws IOException {
        // GIVEN: An empty PaxosLog
        // WHEN: Client queries for non-existent key
        String result = queryValue("title", athens);
        
        // THEN: Should return null, not error
        assertNull("Empty log should return null for non-existent keys", result);
    }

    @Test
    public void demonstratesQueryAfterSet() throws IOException {
        // GIVEN: A value has been set in the log
        executeSetValueCommand("title", "Microservices", athens);
        
        // WHEN: The same key is queried
        String result = queryValue("title", athens);
        
        // THEN: Should return the previously set value
        assertEquals("Query should return previously set value", "Microservices", result);
    }

    // ============================================================================
    // SCENARIO 2: Multiple Commands and Log Growth
    // ============================================================================
    
    @Test
    public void demonstratesMultipleCommandsInSequence() throws IOException {
        // EDUCATIONAL SCENARIO: Build up a log with multiple entries
        LogBuildingResult result = buildMultiCommandLog();
        
        // EDUCATIONAL VERIFICATION: All commands should be in the log
        verifyLogConsistencyAcrossCluster(5, "After building multi-command log");
        
        // EDUCATIONAL FINAL CHECK: Last command result should be accessible
        String finalTitle = queryValue("title", athens);
        assertEquals("Final title should be from last CAS operation", "Testing Paxos ", finalTitle);
    }
    
    private LogBuildingResult buildMultiCommandLog() throws IOException {
        // STEP 1: Set initial title
        String result1 = executeSetValueCommand("title", "Microservices", athens);
        assertEquals("First command should succeed", "Microservices", result1);
        
        // STEP 2: Set second different key  
        String result2 = executeSetValueCommand("title2", "Distributed Systems", byzantium);
        assertEquals("Second command should succeed", "Distributed Systems", result2);
        
        // EDUCATIONAL CHECK: Both commands should create log entries
        verifyLogConsistencyAcrossCluster(2, "After two basic commands");
        
        // STEP 3: Failed CAS operation (existing value doesn't match)
        CompareAndSwapResult cas1 = executeCompareAndSwap("title", null,
                "Microservices", athens);
        assertFalse("CAS should fail when expected value doesn't match", cas1.succeeded);
        assertEquals("CAS should return existing value", "Microservices", cas1.returnedValue);
        
        // STEP 4: Successful CAS operation (existing value matches)
        CompareAndSwapResult cas2 = executeCompareAndSwap("title", "Microservices",
                "Event Driven Microservices", byzantium);
        assertTrue("CAS should succeed when expected value matches", cas2.succeeded);
        assertEquals("CAS should return previous value", "Microservices", cas2.returnedValue);
        
        // STEP 5: Another successful CAS operation
        CompareAndSwapResult cas3 = executeCompareAndSwap("title", "Event Driven Microservices", "Testing Paxos ", byzantium);
        assertTrue("Final CAS should succeed", cas3.succeeded);
        assertEquals("Final CAS should return previous value", "Event Driven Microservices", cas3.returnedValue);
        
        return new LogBuildingResult(5, "Testing Paxos ");
    }

    // ============================================================================
    // SCENARIO 3: Network Partition and Log Reconciliation (Advanced Educational Scenario)
    // This demonstrates the critical PaxosLog conflict resolution mechanism
    // ============================================================================
    
    @Test
    public void demonstratesLogReconciliationAfterNetworkPartition() throws IOException {
        // PHASE 1: Create conflicting incomplete log entries due to network partition
        ConflictingEntriesResult phase1 = createConflictingLogEntries();
        
        // PHASE 2: Heal network and trigger reconciliation
        LogReconciliationResult phase2 = triggerLogReconciliation();
        
        // FINAL VERIFICATION: All nodes should have consistent log
        verifyFinalLogConsistency("CAS Title", 2);
    }
    
    private ConflictingEntriesResult createConflictingLogEntries() throws IOException {
        // EDUCATIONAL SETUP: Athens tries to write but gets isolated
        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);
        
        // EDUCATIONAL ATTEMPT 1: Athens tries "Initial Title" but can't reach quorum
        var athensAttempt = executeSetValueCommandExpectingFailure("title", "Initial Title", athens);
        assertTrue("Athens should fail due to network partition", athensAttempt.failed);
        
        // EDUCATIONAL STATE CHECK: Only Athens has the incomplete entry
        assertEquals("Athens should have 1 incomplete entry", 1, athens.paxosLog.size());
        assertEquals("Byzantium should have empty log", 0, byzantium.paxosLog.size());
        assertEquals("Cyrene should have empty log", 0, cyrene.paxosLog.size());
        
        // EDUCATIONAL SETUP: Byzantium and Cyrene can communicate, but Byzantium has commit issues
        byzantium.dropAfterNMessagesTo(cyrene, 2); // Allow prepare+propose, block commit
        
        // EDUCATIONAL ATTEMPT 2: Byzantium tries "Updated Title" with partial failure
        var byzantiumAttempt = executeSetValueCommandExpectingFailure("title", "Updated Title", byzantium);
        assertTrue("Byzantium should fail due to commit phase issues", byzantiumAttempt.failed);
        
        // EDUCATIONAL STATE CHECK: Byzantium and Cyrene have entry, but uncertainty about commit
        assertEquals("Athens still has 1 entry", 1, athens.paxosLog.size());
        assertEquals("Byzantium should have 1 entry", 1, byzantium.paxosLog.size());
        assertEquals("Cyrene should have 1 entry", 1, cyrene.paxosLog.size());
        
        return new ConflictingEntriesResult("Initial Title", "Updated Title");
    }
    
    private LogReconciliationResult triggerLogReconciliation() throws IOException {
        // EDUCATIONAL SETUP: Network heals
        athens.reconnectTo(cyrene);
        byzantium.reconnectTo(cyrene);
        
        // EDUCATIONAL KEY POINT: Cyrene triggers reconciliation by attempting new command
        // This command will go to index 2, but first index 1 needs to be reconciled
        CompareAndSwapResult casResult = executeCompareAndSwap("title", "Updated Title", "CAS Title", cyrene);
        
        // EDUCATIONAL VERIFICATION: CAS should succeed and return reconciled value
        assertEquals("CAS should return the reconciled value from index 1", 
                    "Updated Title", casResult.returnedValue);
        
        // EDUCATIONAL KEY INSIGHT: Log reconciliation happened automatically
        verifyLogConsistencyAcrossCluster(2, "After reconciliation");
        
        return new LogReconciliationResult("Updated Title", "CAS Title", 2);
    }
    
    private void verifyFinalLogConsistency(String expectedFinalValue, int expectedLogSize) {
        // EDUCATIONAL VERIFICATION: All nodes should have identical logs
        assertEquals("All nodes should have same final value", 
                    expectedFinalValue, athens.getValue("title"));
        assertEquals("All nodes should have same final value", 
                    expectedFinalValue, byzantium.getValue("title"));
        assertEquals("All nodes should have same final value", 
                    expectedFinalValue, cyrene.getValue("title"));
    }

    // ============================================================================
    // SCENARIO 4: Sequential Ordering and Gap Prevention 
    // ============================================================================
    
    @Test
    public void demonstratesSequentialLogOrderingWithGapPrevention() throws IOException, InterruptedException {
        // EDUCATIONAL SCENARIO: Show that logs cannot have gaps
        SequentialOrderingResult result = demonstrateGapPreventionMechanism();
        
        // EDUCATIONAL VERIFICATION: Final log should be complete without gaps
        verifyLogConsistencyAcrossCluster(2, "After gap prevention demonstration");
    }
    
    private SequentialOrderingResult demonstrateGapPreventionMechanism() throws IOException, InterruptedException {
        // EDUCATIONAL SETUP: Create incomplete entry at index 0
        athens.dropAfterNMessagesTo(byzantium, 1); // Prepare succeeds, propose fails
        athens.dropMessagesTo(cyrene);
        
        var incompleteAttempt = executeSetValueCommandExpectingFailure("title", "Microservices", athens);
        assertTrue("First command should fail due to network partition", incompleteAttempt.failed);
        
        // EDUCATIONAL STATE: Athens has incomplete entry, others are empty
        assertEquals("Athens should have incomplete entry", 1, athens.paxosLog.size());
        assertEquals("Byzantium should have partial entry", 1, byzantium.paxosLog.size());
        assertEquals("Cyrene should be empty", 0, cyrene.paxosLog.size());
        
        // EDUCATIONAL SETUP: Network partially heals
        athens.reconnectTo(cyrene);
        
        // EDUCATIONAL KEY POINT: New command must complete index 0 before proceeding to index 1
        String result = executeSetValueCommand("newTitle", "Event Driven Microservices", cyrene);
        assertEquals("New command should succeed", "Event Driven Microservices", result);
        
        // EDUCATIONAL VERIFICATION: Gap prevention worked - no holes in log
        verifyLogConsistencyAcrossCluster(2, "After gap prevention");
        
        return new SequentialOrderingResult("Event Driven Microservices", 2, true);
    }

    // ============================================================================
    // Educational Utility Methods - Making tests more readable and teachable
    // ============================================================================
    
    private String executeSetValueCommand(String key, String value, PaxosLog node) throws IOException {
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand(key, value).serialize();
        var response = networkClient.sendAndReceive(
            new ExecuteCommandRequest(command), 
            node.getClientConnectionAddress(), 
            ExecuteCommandResponse.class
        ).getResult();
        
        return response.getResponse().orElse(null);
    }
    
    private CommandExecutionResult executeSetValueCommandExpectingFailure(String key, String value, PaxosLog node) throws IOException {
        var networkClient = new NetworkClient();
        byte[] command = new SetValueCommand(key, value).serialize();
        var response = networkClient.sendAndReceive(
            new ExecuteCommandRequest(command), 
            node.getClientConnectionAddress(), 
            ExecuteCommandResponse.class
        );
        
        return new CommandExecutionResult(key, value, response.isError());
    }
    
    private CompareAndSwapResult executeCompareAndSwap(String key, String expectedValue, String newValue, PaxosLog node) throws IOException {
        var networkClient = new NetworkClient();
        CompareAndSwap casCommand = new CompareAndSwap(key, Optional.ofNullable(expectedValue), newValue);
        var response = networkClient.sendAndReceive(
            new ExecuteCommandRequest(casCommand.serialize()), 
            node.getClientConnectionAddress(), 
            ExecuteCommandResponse.class
        ).getResult();
        
        return new CompareAndSwapResult(
            response.isCommitted(), 
            response.getResponse().orElse(null)
        );
    }
    
    private String queryValue(String key, PaxosLog node) throws IOException {
        var networkClient = new NetworkClient();
        var response = networkClient.sendAndReceive(
            new GetValueRequest(key), 
            node.getClientConnectionAddress(), 
            GetValueResponse.class
        ).getResult();
        
        return response.value.orElse(null);
    }
    
    private void verifyLogConsistencyAcrossCluster(int expectedSize, String context) {
        assertEquals(context + " - Athens log size", expectedSize, athens.paxosLog.size());
        assertEquals(context + " - Byzantium log size", expectedSize, byzantium.paxosLog.size());
        assertEquals(context + " - Cyrene log size", expectedSize, cyrene.paxosLog.size());
    }

    // ============================================================================
    // Educational Data Classes - Making test results more structured and readable
    // ============================================================================
    
    private static class CommandExecutionResult {
        final String key;
        final String value;
        final boolean failed;
        
        CommandExecutionResult(String key, String value, boolean failed) {
            this.key = key;
            this.value = value;
            this.failed = failed;
        }
    }
    
    private static class CompareAndSwapResult {
        final boolean succeeded;
        final String returnedValue;
        
        CompareAndSwapResult(boolean succeeded, String returnedValue) {
            this.succeeded = succeeded;
            this.returnedValue = returnedValue;
        }
    }
    
    private static class LogBuildingResult {
        final int finalLogSize;
        final String finalValue;
        
        LogBuildingResult(int finalLogSize, String finalValue) {
            this.finalLogSize = finalLogSize;
            this.finalValue = finalValue;
        }
    }
    
    private static class ConflictingEntriesResult {
        final String athensAttemptedValue;
        final String byzantiumAttemptedValue;
        
        ConflictingEntriesResult(String athensAttemptedValue, String byzantiumAttemptedValue) {
            this.athensAttemptedValue = athensAttemptedValue;
            this.byzantiumAttemptedValue = byzantiumAttemptedValue;
        }
    }
    
    private static class LogReconciliationResult {
        final String reconciledValue;
        final String finalValue;
        final int finalLogSize;
        
        LogReconciliationResult(String reconciledValue, String finalValue, int finalLogSize) {
            this.reconciledValue = reconciledValue;
            this.finalValue = finalValue;
            this.finalLogSize = finalLogSize;
        }
    }
    
    private static class SequentialOrderingResult {
        final String finalValue;
        final int logSize;
        final boolean gapPreventionWorked;
        
        SequentialOrderingResult(String finalValue, int logSize, boolean gapPreventionWorked) {
            this.finalValue = finalValue;
            this.logSize = logSize;
            this.gapPreventionWorked = gapPreventionWorked;
        }
    }
}