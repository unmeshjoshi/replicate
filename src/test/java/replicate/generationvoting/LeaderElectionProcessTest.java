package replicate.generationvoting;

import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.TestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

/**
 * LeaderElectionProcessTest demonstrates the step-by-step leader election process
 * with generation numbers as described in the consensus introduction slides.
 * 
 * This test corresponds to the "leader_election_process" diagram reference and shows:
 * 1. Athens chooses Generation Number 1 (higher than any seen)
 * 2. Sends prepare requests to majority (self + Byzantium)
 * 3. Nodes promise not to accept requests with lower generation numbers
 * 4. Athens becomes Leader for Generation Number 1
 * 
 * The test validates the mathematical foundation of consensus algorithms:
 * - Generation numbers establish clear ordering and authority
 * - Majority quorums ensure consistency
 * - Promise mechanism prevents conflicting leadership
 */
public class LeaderElectionProcessTest extends ClusterTest<GenerationVoting> {

    @Test
    public void demonstratesStepByStepLeaderElectionProcess() throws Exception {
        // Setup: Start a 3-node cluster (Athens, Byzantium, Cyrene)
        // This matches exactly the scenario from leader_election_process.puml
        super.nodes = TestUtils.startCluster(
            Arrays.asList("athens", "byzantium", "cyrene"), 
            (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
                -> new GenerationVoting(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
        );
        
        GenerationVoting athens = nodes.get("athens");
        GenerationVoting byzantium = nodes.get("byzantium");
        GenerationVoting cyrene = nodes.get("cyrene");

        System.out.println("=== LEADER ELECTION PROCESS TEST ===");
        System.out.println("Scenario: Athens wants to become leader and chooses Generation Number 1");
        
        // Step 1: Verify initial state - no promises made yet
        assertEquals("Athens should start with generation 0", 0, athens.generation);
        assertEquals("Byzantium should start with generation 0", 0, byzantium.generation);
        assertEquals("Cyrene should start with generation 0", 0, cyrene.generation);
        
        System.out.println("✓ Initial state: Athens(0), Byzantium(0), Cyrene(0)");

        // Step 2: Athens wants to become leader (matches diagram exactly)
        // This exactly matches the diagram note: "Wants to become leader, Chooses Generation Number 1 (higher than any seen)"
        System.out.println("\n--- PHASE 1: PREPARE (Generation Number Selection) ---");
        System.out.println("Athens wants to become leader");
        System.out.println("Athens chooses Generation Number 1 (higher than any seen)");
        
        // Step 3: Athens initiates leader election directly
        // This is much more natural and matches the diagram intention
        System.out.println(" Athens -> Byzantium: Prepare(generation=1)");
        System.out.println(" Athens -> Cyrene: Prepare(generation=1)");
        
        // Athens runs election - will propose generation number higher than current (0)
        CompletableFuture<Integer> leaderElectionResult = athens.runElection();
        Integer electedGeneration = leaderElectionResult.get(); // Wait for completion
        
        // Step 4: Verify Promise responses
        // This matches: "Byzantium -> Athens: Promise(generation=1)" and "Cyrene -> Athens: Promise(generation=1)"
        System.out.println("\n--- PHASE 2: PROMISE (Majority Agreement) ---");
        System.out.println(" Byzantium checks: 1 > any promised? Yes! Promises not to accept requests with generation < 1");
        System.out.println(" Cyrene checks: 1 > any promised? Yes! Promises not to accept requests with generation < 1");
        System.out.println(" Byzantium -> Athens: Promise(generation=1)");
        System.out.println(" Cyrene -> Athens: Promise(generation=1)");
        
        // Step 5: Athens becomes leader
        // This matches: "Got majority promises! Now Leader for Generation 1 Can proceed with Phase 2"
        assertEquals("Athens should be elected with generation 1", 1, electedGeneration.intValue());
        assertEquals("Athens should have generation 1", 1, athens.generation);
        assertEquals("Byzantium should have promised generation 1", 1, byzantium.generation);
        assertEquals("Cyrene should have promised generation 1", 1, cyrene.generation);
        
        System.out.println("\n--- LEADERSHIP ESTABLISHED ---");
        System.out.println("Athens got majority promises!");
        System.out.println("Athens is now Leader for Generation Number 1");
        System.out.println("Athens can proceed with Phase 2 (Accept/Commit)");
        
        // Final state verification matching the diagram outcome
        System.out.println("\n--- FINAL STATE VERIFICATION ---");
        System.out.printf("Athens (Leader): generation=%d%n", athens.generation);
        System.out.printf("Byzantium (Acceptor): generation=%d%n", byzantium.generation);  
        System.out.printf("Cyrene (Acceptor): generation=%d%n", cyrene.generation);
        
        System.out.println("\n✓ Leader election process completed successfully!");
        System.out.println("\nKey Insights Demonstrated (matching leader_election_process.puml):");
        System.out.println("1. Candidate chooses generation number higher than any seen");
        System.out.println("2. Prepare requests sent to all acceptors");
        System.out.println("3. Acceptors promise not to accept lower generation numbers");
        System.out.println("4. Majority promises establish leadership authority");
        System.out.println("5. Leader can now proceed with consensus (Phase 2)");
    }

    @Test
    public void demonstratesLeaderElectionWithNetworkPartition() throws Exception {
        // This test shows what happens when a node tries to become leader
        // but cannot reach the full majority due to network issues
        super.nodes = TestUtils.startCluster(
            Arrays.asList("athens", "byzantium", "cyrene"), 
            (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
                -> new GenerationVoting(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
        );
        
        GenerationVoting athens = nodes.get("athens");
        GenerationVoting byzantium = nodes.get("byzantium");
        GenerationVoting cyrene = nodes.get("cyrene");

        System.out.println("\n=== LEADER ELECTION WITH NETWORK PARTITION TEST ===");
        
        // Step 1: Create network partition - Athens cannot reach Cyrene
        System.out.println("Creating network partition: Athens cannot communicate with Cyrene");
        athens.dropMessagesTo(cyrene);
        cyrene.dropMessagesTo(athens);
        
        // Step 2: Athens attempts leader election but can only reach Byzantium
        // This should still succeed since Athens + Byzantium = majority (2 out of 3)
        System.out.println("Athens attempts leader election with partial connectivity");
        System.out.println("Athens can only reach Byzantium (majority = 2 out of 3)");
        
        CompletableFuture<Integer> leaderElectionResult = athens.runElection();
        Integer electedGeneration = leaderElectionResult.get();
        
        // Step 3: Verify partial quorum success
        assertEquals("Athens should succeed with generation 1", 1, electedGeneration.intValue());
        assertEquals("Athens should have generation 1", 1, athens.generation);
        assertEquals("Byzantium should have generation 1", 1, byzantium.generation);
        assertEquals("Cyrene should remain at generation 0 (partitioned)", 0, cyrene.generation);
        
        System.out.println("✓ Leader election succeeded with partial quorum");
        System.out.printf("Final state: Athens(1), Byzantium(1), Cyrene(0)%n");
        
        System.out.println("\nKey Insight: Majority quorum allows progress despite failures");
    }

    @Test
    public void demonstratesCompetingLeaderElection() throws Exception {
        // This test shows what happens when multiple nodes try to become leader
        // simultaneously, demonstrating how generation numbers resolve conflicts
        super.nodes = TestUtils.startCluster(
            Arrays.asList("athens", "byzantium", "cyrene"), 
            (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
                -> new GenerationVoting(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses)
        );
        
        GenerationVoting athens = nodes.get("athens");
        GenerationVoting byzantium = nodes.get("byzantium");
        GenerationVoting cyrene = nodes.get("cyrene");

        System.out.println("\n=== COMPETING LEADER ELECTION TEST ===");
        
        // Step 1: Athens becomes leader first
        System.out.println("Athens initiates first leader election");
        CompletableFuture<Integer> athensResult = athens.runElection();
        Integer athensGeneration = athensResult.get();
        
        assertEquals("Athens should get generation 1", 1, athensGeneration.intValue());
        System.out.println("✓ Athens elected as leader with generation 1");
        
        // Step 2: Byzantium attempts to become leader
        // It should get a higher generation number since it sees Athens' generation 1
        System.out.println("Byzantium attempts to become leader");
        CompletableFuture<Integer> byzantiumResult = byzantium.runElection();
        Integer byzantiumGeneration = byzantiumResult.get();
        
        assertEquals("Byzantium should get generation 2", 2, byzantiumGeneration.intValue());
        System.out.println("Byzantium elected as leader with generation 2");
        
        // Step 3: Athens tries again - should get even higher generation
        System.out.println("Athens attempts to regain leadership");
        CompletableFuture<Integer> athensSecondResult = athens.runElection();
        Integer athensSecondGeneration = athensSecondResult.get();
        
        assertEquals("Athens should get generation 3", 3, athensSecondGeneration.intValue());
        System.out.println("Athens regains leadership with generation 3");
        
        System.out.println("\nKey Insight: Generation numbers prevent conflicting leadership");
        System.out.println("Higher generation numbers always take precedence");
    }
} 