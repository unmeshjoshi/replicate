package replicate.examples;

import org.junit.Test;
import replicate.common.Config;
import replicate.common.TestUtils;
import replicate.wal.DurableKVStore;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * SingleServerProblemTest demonstrates the fundamental single point of failure problem
 * that motivates the need for distributed systems and consensus algorithms.
 * 
 * This test corresponds to the "single_server_problem" diagram reference and shows:
 * 1. Single server working normally - all requests processed successfully
 * 2. Single server failure - all clients affected, complete system unavailability
 * 
 * This is the foundation for understanding "why consensus?" - we need distributed
 * systems to eliminate single points of failure.
 */
public class SingleServerProblemTest {

    @Test
    public void demonstratesSingleServerProblem() throws IOException {
        System.out.println("=== SINGLE SERVER PROBLEM DEMONSTRATION ===");
        System.out.println("Scenario: Single server handling all client requests");
        
        // Setup: Single server (representing traditional single-server architecture)
        File walDir = TestUtils.tempDir("single-server-test");
        Config config = new Config(walDir.getAbsolutePath());
        DurableKVStore server = new DurableKVStore(config);
        
        System.out.println("Initial setup: Single server running, two clients (Alice and Bob)");
        
        // === STEP 1: Single Server Working ===
        System.out.println("\n--- STEP 1: Single Server Working ---");
        System.out.println("When server is healthy, all requests are processed");
        
        // Alice sends request
        System.out.println("Alice -> Server: SetValue(title, 'Alice Book')");
        server.put("title", "Alice Book");
        System.out.println("Server -> Alice: Success");
        
        // Bob sends request  
        System.out.println("Bob -> Server: SetValue(author, 'Bob Writer')");
        server.put("author", "Bob Writer");
        System.out.println("Server -> Bob: Success");
        
        // Verify both operations succeeded
        assertEquals("Alice Book", server.get("title"));
        assertEquals("Bob Writer", server.get("author"));
        
        System.out.println("Result: Both clients successfully processed");
        System.out.println("Server state: title='Alice Book', author='Bob Writer'");
        
        // === STEP 2: Single Server Failure ===
        System.out.println("\n--- STEP 2: Single Server Failure ---");
        System.out.println("Server is down - simulating server failure");
        
        // Simulate server failure by stopping it
        server.close();
        
        System.out.println("Alice -> Server: GetValue(title)");
        try {
            String failedResponse = server.get("title");
            // After close(), the internal map is cleared, so this will return null
            assertNull("Server should return null after failure", failedResponse);
            System.out.println("Server -> Alice: Service Unavailable (Returns null - data lost)");
        } catch (Exception e) {
            System.out.println("Server -> Alice: Service Unavailable (Request fails)");
            System.out.println("Error: " + e.getClass().getSimpleName());
        }
        
        System.out.println("Bob -> Server: GetValue(author)");
        try {
            String failedResponse = server.get("author");
            // After close(), the internal map is cleared, so this will return null
            assertNull("Server should return null after failure", failedResponse);
            System.out.println("Server -> Bob: Service Unavailable (Returns null - data lost)");
        } catch (Exception e) {
            System.out.println("Server -> Bob: Service Unavailable (Request fails)");
            System.out.println("Error: " + e.getClass().getSimpleName());
        }
        
        System.out.println("\n--- CONCLUSION: Single Point of Failure ---");
        System.out.println("When the only server fails, ALL clients are affected");
        System.out.println("No way to access previously stored data");
        System.out.println("Complete system unavailability");
        
        System.out.println("\nKey Insight: This motivates the need for distributed systems!");
        System.out.println("Solution: Multiple servers working together with consensus algorithms");
    }
    
    @Test
    public void demonstratesDataLossOnServerFailure() throws IOException {
        System.out.println("\n=== DATA LOSS SCENARIO ===");
        System.out.println("Demonstrating data loss when single server fails");
        
        // Setup single server
        File walDir = TestUtils.tempDir("data-loss-test");
        Config config = new Config(walDir.getAbsolutePath());
        DurableKVStore server = new DurableKVStore(config);
        
        // Store important data
        System.out.println("Storing critical business data...");
        server.put("customer-count", "10000");
        server.put("revenue", "1000000");
        server.put("last-backup", "2024-01-01");
        
        System.out.println("Data stored: customer-count=10000, revenue=1000000");
        
        // Verify data is there
        assertEquals("10000", server.get("customer-count"));
        assertEquals("1000000", server.get("revenue"));
        
        // Server crashes (simulating hardware failure, power outage, etc.)
        System.out.println("\nSERVER CRASH: Hardware failure, power outage, or system crash");
        server.close();
        
        // Attempt to recover data
        System.out.println("Attempting to recover critical business data...");
        
        String customerCount = server.get("customer-count");
        assertNull("Data should be lost after server failure", customerCount);
        System.out.println("CRITICAL: Cannot access customer-count data - returns null");
        
        String revenue = server.get("revenue");
        assertNull("Data should be lost after server failure", revenue);
        System.out.println("CRITICAL: Cannot access revenue data - returns null");
        
        System.out.println("\nBUSINESS IMPACT:");
        System.out.println("- Cannot serve customers (no access to customer data)");
        System.out.println("- Cannot process transactions (no access to financial data)");
        System.out.println("- Data may be permanently lost if no recent backups");
        System.out.println("- Business operations completely halted");
        
        System.out.println("\nThis demonstrates why single-server architectures are inadequate");
        System.out.println("for critical business applications requiring high availability.");
    }
} 