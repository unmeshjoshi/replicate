package replicate.examples;

import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.twophaseexecution.CompareAndSwap;
import replicate.twophaseexecution.SinglePhaseExecution;
import replicate.twophaseexecution.messages.ExecuteCommandRequest;
import replicate.twophaseexecution.messages.ExecuteCommandResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * SinglePhaseExecutionFailureTest demonstrates why single-phase execution
 * in distributed systems leads to inconsistencies and data loss.
 * 
 * This test corresponds exactly to the "single_phase_execution" diagram and shows:
 * 1. Initial setup - Athens, Byzantium, Cyrene all have Counter=5
 * 2. Client sends increment request to Athens
 * 3. Athens executes immediately: Counter += 1 (Counter=6)
 * 4. Athens tries to propagate but network issues cause delays/losses
 * 5. Athens responds "Success" to client despite incomplete propagation
 * 6. Athens crashes before updates reach other nodes
 * 7. System is inconsistent - Athens had Counter=6 (crashed), others have Counter=5
 * 8. Client confused when reading Counter=5 from Byzantium despite "successful" increment
 * 
 * This is the core motivation scenario for two-phase commit protocols and consensus.
 */
public class SinglePhaseExecutionFailureTest extends ClusterTest<SinglePhaseExecution> {

    @Test
    public void demonstratesSinglePhaseExecutionFailure() throws IOException, InterruptedException {
        System.out.println("=== SINGLE PHASE EXECUTION FAILURE DEMONSTRATION ===");
        System.out.println("Scenario: Exactly matching single_phase_execution.puml diagram");
        
        // === STEP 1: Initial Setup ===
        System.out.println("\n--- STEP 1: Initial Setup ---");
        System.out.println("Athens(Counter=5), Byzantium(Counter=5), Cyrene(Counter=5)");
        
        super.nodes = TestUtils.startCluster(
            Arrays.asList("athens", "byzantium", "cyrene"),
            (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
                -> new SinglePhaseExecution(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
        );
        
        SinglePhaseExecution athens = nodes.get("athens");
        SinglePhaseExecution byzantium = nodes.get("byzantium");
        SinglePhaseExecution cyrene = nodes.get("cyrene");
        
        NetworkClient client = new NetworkClient();
        
        // Initialize all nodes with Counter=5
        CompareAndSwap initCommand = new CompareAndSwap("counter", Optional.empty(), "5");
        ExecuteCommandResponse initResponse = client.sendAndReceive(
            new ExecuteCommandRequest(initCommand.serialize()),
            athens.getClientConnectionAddress(),
            ExecuteCommandResponse.class
        ).getResult();
        
        assertTrue("Initial setup should succeed", initResponse.isCommitted());
        
        // Wait for propagation to complete
        TestUtils.waitUntilTrue(() -> {
            return "5".equals(athens.getValue("counter")) &&
                   "5".equals(byzantium.getValue("counter")) &&
                   "5".equals(cyrene.getValue("counter"));
        }, "Waiting for counter initialization to propagate", Duration.ofSeconds(2));
        
        System.out.println("Athens: Counter=" + athens.getValue("counter"));
        System.out.println("Byzantium: Counter=" + byzantium.getValue("counter"));
        System.out.println("Cyrene: Counter=" + cyrene.getValue("counter"));
        
        // === STEP 2: Client sends increment request ===
        System.out.println("\n--- STEP 2: Client sends increment request ---");
        System.out.println("Alice (Client) -> Athens: IncrementCounter");
        
        // === STEP 3: Athens executes immediately ===
        System.out.println("\n--- STEP 3: Athens executes immediately ---");
        System.out.println("Athens: Counter += 1");
        
        // === STEP 4: Athens tries to propagate (but we simulate network issues) ===
        System.out.println("\n--- STEP 4: Athens tries to propagate ---");
        System.out.println("Athens -> Byzantium: Update Counter (delayed/lost)");
        System.out.println("Athens -> Cyrene: Update Counter (delayed/lost)");
        System.out.println("Network issues prevent updates from reaching other nodes");
        
        // Simulate network issues by dropping messages AFTER Athens executes but BEFORE propagation
        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);
        
        // === STEP 5: Athens responds to client ===
        System.out.println("\n--- STEP 5: Athens responds to client ---");
        CompareAndSwap incrementCommand = new CompareAndSwap("counter", Optional.of("5"), "6");
        ExecuteCommandResponse response = client.sendAndReceive(
            new ExecuteCommandRequest(incrementCommand.serialize()),
            athens.getClientConnectionAddress(),
            ExecuteCommandResponse.class
        ).getResult();
        
        // With single-phase execution, Athens responds Success even though propagation failed
        assertTrue("Athens responds Success despite failed propagation", response.isCommitted());
        System.out.println("Athens -> Alice: Success");
        
        // Athens now has Counter=6, but others still have Counter=5
        assertEquals("6", athens.getValue("counter"));
        assertEquals("5", byzantium.getValue("counter"));
        assertEquals("5", cyrene.getValue("counter"));
        
        System.out.println("Athens: Counter=" + athens.getValue("counter"));
        System.out.println("Byzantium: Counter=" + byzantium.getValue("counter") + " (unchanged)");
        System.out.println("Cyrene: Counter=" + cyrene.getValue("counter") + " (unchanged)");
        
        // === STEP 6: Athens fails ===
        System.out.println("\n--- STEP 6: Athens fails ---");
        System.out.println("Athens crashes");
        athens.close();
        
        // === STEP 7: System state is now inconsistent ===
        System.out.println("\n--- STEP 7: System state is now inconsistent ---");
        System.out.println("Athens: Counter=6 (crashed)");
        System.out.println("Byzantium: Counter=" + byzantium.getValue("counter"));
        System.out.println("Cyrene: Counter=" + cyrene.getValue("counter"));
        System.out.println("System is now inconsistent!");
        System.out.println("Athens had Counter=6, Byzantium and Cyrene still have Counter=5");
        
        // === STEP 8: Client confused about true value ===
        System.out.println("\n--- STEP 8: Client confused about true value ---");
        System.out.println("Alice -> Byzantium: ReadCounter");
        
        String byzantiumValue = byzantium.getValue("counter");
        System.out.println("Byzantium -> Alice: Counter=" + byzantiumValue);
        
        assertEquals("5", byzantiumValue);
        
        System.out.println("Client sees Counter=5 despite previous 'successful' increment operation!");
        
        System.out.println("\n--- CONCLUSION: Single Phase Execution Problems ---");
        System.out.println("1. Athens executed immediately without waiting for consensus");
        System.out.println("2. Athens responded 'Success' before ensuring propagation");
        System.out.println("3. Network issues prevented updates from reaching other nodes");
        System.out.println("4. Athens crash caused data loss and inconsistency");
        System.out.println("5. Client received misleading 'Success' response");
        
        System.out.println("\nKey Insight: This motivates two-phase commit protocols!");
        System.out.println("Solution: Get consensus BEFORE execution, not after");
    }
    
    @Test
    public void demonstratesNetworkPartitionDuringPropagation() throws IOException {
        System.out.println("\n=== NETWORK PARTITION DURING PROPAGATION ===");
        System.out.println("Showing what happens when propagation is delayed/lost");
        
        super.nodes = TestUtils.startCluster(
            Arrays.asList("athens", "byzantium", "cyrene"),
            (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
                -> new SinglePhaseExecution(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
        );
        
        SinglePhaseExecution athens = nodes.get("athens");
        SinglePhaseExecution byzantium = nodes.get("byzantium");
        SinglePhaseExecution cyrene = nodes.get("cyrene");
        
        NetworkClient client = new NetworkClient();
        
        // Initialize all nodes
        CompareAndSwap initCommand = new CompareAndSwap("value", Optional.empty(), "initial");
        ExecuteCommandResponse initResponse = client.sendAndReceive(
            new ExecuteCommandRequest(initCommand.serialize()),
            athens.getClientConnectionAddress(),
            ExecuteCommandResponse.class
        ).getResult();
        
        assertTrue("Initial setup should succeed", initResponse.isCommitted());
        
        // Wait for initial value to propagate to all nodes
        TestUtils.waitUntilTrue(() -> {
            return "initial".equals(athens.getValue("value")) &&
                   "initial".equals(byzantium.getValue("value")) &&
                   "initial".equals(cyrene.getValue("value"));
        }, "Waiting for value initialization to propagate", Duration.ofSeconds(2));
        
        System.out.println("All nodes initialized: value='initial'");
        
        // Athens executes an update but propagation is blocked
        System.out.println("\nClient sends update to Athens");
        System.out.println("Athens executes immediately and responds Success");
        System.out.println("But network partition prevents propagation to other nodes");
        
        // Block propagation
        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);
        
        CompareAndSwap updateCommand = new CompareAndSwap("value", Optional.of("initial"), "updated");
        ExecuteCommandResponse updateResponse = client.sendAndReceive(
            new ExecuteCommandRequest(updateCommand.serialize()),
            athens.getClientConnectionAddress(),
            ExecuteCommandResponse.class
        ).getResult();
        
        assertTrue("Athens responds Success despite failed propagation", updateResponse.isCommitted());
        
        // System is now inconsistent
        assertEquals("updated", athens.getValue("value"));
        assertEquals("initial", byzantium.getValue("value"));
        assertEquals("initial", cyrene.getValue("value"));
        
        System.out.println("Athens: value='updated'");
        System.out.println("Byzantium: value='initial'");
        System.out.println("Cyrene: value='initial'");
        System.out.println("System is inconsistent!");
        
        System.out.println("\nThis demonstrates the core problem with single-phase execution:");
        System.out.println("- Immediate execution before consensus");
        System.out.println("- Optimistic propagation that can fail silently");
        System.out.println("- False success responses to clients");
        System.out.println("- Inevitable inconsistency during network issues");
    }
} 