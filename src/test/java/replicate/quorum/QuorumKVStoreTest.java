package replicate.quorum;

import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.TestUtils;
import replicate.net.InetAddressAndPort;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class QuorumKVStoreTest extends ClusterTest<QuorumKVStore> {
    QuorumKVStore athens;
    QuorumKVStore byzantium;
    QuorumKVStore cyrene;

    @Override
    public void setUp() throws IOException {
        this.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress,peerAddresses));

        athens = nodes.get("athens");
        byzantium = nodes.get("byzantium");
        cyrene = nodes.get("cyrene");
    }
     //Read Your Own Writes should give the same value written by me or a later value.
    //Try changing this test to have 5 replicas instead of three.
    //It returns error because Quorum condition will not be met.
     @Test
    public void quorumReadWriteTest() throws IOException {

        athens.dropMessagesTo(byzantium);

        InetAddressAndPort athensAddress = athens.getClientConnectionAddress();
        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athensAddress, "title", "Microservices");
        assertEquals("Success", response);

        //how to make sure replicas are in sync?

        String value = kvClient.getValue(athensAddress, "title");

        assertEquals("Microservices", value);


        assertEquals("Microservices", athens.get("title").getValue());
    }


    @Test
    public void quorumReadFailsWhenSynchronousReadRepairFails() throws IOException {

        athens.dropMessagesTo(byzantium);

        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Success", response);

        assertEquals("Microservices", athens.get("title").getValue());
        assertEquals("Microservices", cyrene.get("title").getValue());
        assertEquals("", byzantium.get("title").getValue());

        cyrene.dropMessagesTo(athens);
        cyrene.dropAfterNMessagesTo(byzantium, 1);
        //cyrene will read from itself and byzantium. byzantium has stale value, so it will try read-repair.
        //but read-repair call fails.
        String value = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Error", value);
        assertEquals("", byzantium.get("title").getValue());
    }


    @Test
    public void quorumReadRepairUpdatesStaleValues() throws IOException {
        //setup initial value.
        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Initial title");
        assertEquals("Success", response);

        athens.dropMessagesTo(byzantium);

        response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Updated title");
        assertEquals("Success", response);

        assertEquals("Updated title", athens.get("title").getValue());
        assertEquals("Updated title", cyrene.get("title").getValue());
        assertEquals("Initial title", byzantium.get("title").getValue());


        String value = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Updated title", value);

        assertEquals("Updated title", byzantium.get("title").getValue());
    }

    @Test
    public void quorumReadCanGetIncompletelyWrittenValues() throws IOException {
        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Error", response);
        assertEquals("Microservices", athens.get("title").getValue());

        athens.reconnectTo(cyrene);
        cyrene.dropMessagesTo(byzantium);

        String value = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Microservices", value);

    }

    //With async read-repair, a client reading after another client can see older values.
    @Test
    public void laterReadsGetOlderValue() throws IOException {
        athens.doAsyncReadRepair();

        KVClient kvClient = new KVClient();
        //Nathan
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Nicroservices");
        assertEquals("Success", response);

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        //Philip
        response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Error", response);


        assertEquals("Microservices", athens.get("title").getValue());
        assertEquals("Nicroservices", byzantium.get("title").getValue());
        assertEquals("Nicroservices", cyrene.get("title").getValue());

        athens.reconnectTo(cyrene);
        athens.addDelayForMessagesTo(cyrene, 1);

        //concurrent read
        //read-repair message to cyrene is delayed..
        //Alice -   //Microservices:timestamp 2 athens
                    //Nitroservices:timestamp 1 cyrene
        //triggers read-repair
        // return "Microservices"
        // GC Pause on athens
        // as part of get request --ReadRepair(SetValue title, "Microservices", ts=2)->cyrene //happens async and delayed.
        String value = kvClient.getValue(athens.getClientConnectionAddress(), "title");
        assertEquals("Microservices", value);

        //concurrent read
        //there is a possibility of this happening.
        //Bob is reading after Alice. But still Bob gets older value.
        //Bob    //Nitroservices:timestamp 1 byzantium
                 //Nitroservices:timestamp 1 cyrene
        value = kvClient.getValue(byzantium.getClientConnectionAddress(), "title");
        assertEquals("Nicroservices", value);

        TestUtils.waitUntilTrue(()->{
            try {
                return "Microservices".equals(kvClient.getValue(byzantium.getClientConnectionAddress(), "title"));
            } catch (IOException e) {
                return false;
            }
        }, "Waiting for read-repair to complete", Duration.ofSeconds(5));
    }

    //Incomplete write requests cause two different clients to see different values
    //depending on which nodes they connect to.
    @Test
    public void compareAndSwapIsSuccessfulForTwoConcurrentClients() throws IOException {

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        replicate.quorumconsensus.KVClient kvClient = new replicate.quorumconsensus.KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Nitroservices");
        assertEquals("Error", response);
        //quorum responses not received as messages to byzantium and cyrene fail.

        assertEquals("Nitroservices", athens.get("title").getValue());
        assertEquals(StoredValue.EMPTY, byzantium.get("title"));
        assertEquals(StoredValue.EMPTY, cyrene.get("title"));

        replicate.quorumconsensus.KVClient alice = new replicate.quorumconsensus.KVClient();

        //cyrene should be able to connect with itself and byzantium.
        //both cyrene and byzantium have empty value.
        //Alice starts the compareAndSwap
        //Alice reads the value.
        String aliceValue = alice.getValue(cyrene.getClientConnectionAddress(), "title");
        //get-compare-modify-write
        //meanwhile bob starts compareAndSwap as well
        //Bob connects to athens, which is now able to connect to cyrene and byzantium
        replicate.quorumconsensus.KVClient bob = new replicate.quorumconsensus.KVClient();
        athens.reconnectTo(cyrene);
        athens.reconnectTo(byzantium);
        String bobValue = bob.getValue(athens.getClientConnectionAddress(), "title");
        if (bobValue.equals("Microservices")) {
            kvClient.setValue(athens.getClientConnectionAddress(), "title", "Distributed Systems");
        }
        //Bob successfully completes compareAndSwap

        //Alice checks the value to be empty.
        if (aliceValue.equals("")) {
            alice.setValue(cyrene.getClientConnectionAddress(), "title", "Nitroservices");
        }
        //Alice successfully completes compareAndSwap

        //Bob is surprised to read the different value after his compareAndSwap was successful.
        response = bob.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Nitroservices", response);
    }
}