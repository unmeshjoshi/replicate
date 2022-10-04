package quorum;

import common.TestUtils;
import distrib.patterns.common.Config;
import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.SystemClock;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.quorum.QuorumKVStore;
import distrib.patterns.quorum.StoredValue;
import distrib.patterns.quorum.messages.SetValueResponse;
import distrib.patterns.quorumconsensus.QuorumKV;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class QuorumReadWriteTest {
     //Read Your Own Writes should give the same value written by me or a later value.
    //Monotonic Reads
    //Monotonic Reads across clients without relying on system timestamp. //linearizable reads
    @Test
    public void quorumReadWriteTest() throws IOException {
        Map<String, QuorumKVStore> kvStores = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress,peerAddresses));

        QuorumKVStore athens = kvStores.get("athens");
        QuorumKVStore byzantium = kvStores.get("byzantium");
        QuorumKVStore cyrene = kvStores.get("cyrene");


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
    public void quorumReadRepairTest() throws IOException {
        Map<String, QuorumKVStore> kvStores = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress,peerAddresses));

        QuorumKVStore athens = kvStores.get("athens");
        QuorumKVStore byzantium = kvStores.get("byzantium");
        QuorumKVStore cyrene = kvStores.get("cyrene");



        athens.dropMessagesTo(byzantium);

        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Success", response);

        assertEquals("Microservices", athens.get("title").getValue());
        assertEquals("Microservices", cyrene.get("title").getValue());
        assertEquals("", byzantium.get("title").getValue());

        cyrene.dropMessagesTo(athens);
        String value = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Microservices", value);

        TestUtils.waitUntilTrue(()-> {
                return "Microservices".equals(byzantium.get("title").getValue());
                }, "Waiting for read repair", Duration.ofSeconds(5));

    }

    @Test
    public void quorumSynchronousReadRepair() throws IOException {
        Map<String, QuorumKVStore> kvStores = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress,peerAddresses));

        QuorumKVStore athens = kvStores.get("athens");
        QuorumKVStore byzantium = kvStores.get("byzantium");
        QuorumKVStore cyrene = kvStores.get("cyrene");


        athens.dropMessagesTo(byzantium);

        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Success", response);

        assertEquals("Microservices", athens.get("title").getValue());
        assertEquals("Microservices", cyrene.get("title").getValue());
        assertEquals("", byzantium.get("title").getValue());

        cyrene.dropMessagesTo(athens);
        cyrene.dropMessagesToAfter(byzantium, 1);
        //cyrene will read from itself and byzantium. byzantium has stale value, so it will try read-repair.
        //but read-repair call fails.
        String value = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Error", value);
        assertEquals("", byzantium.get("title").getValue());
    }

    @Test
    public void quorumReadFailsIfReadRepairFails() throws IOException {
        Map<String, QuorumKVStore> kvStores = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress,peerAddresses));

        QuorumKVStore athens = kvStores.get("athens");
        QuorumKVStore byzantium = kvStores.get("byzantium");
        QuorumKVStore cyrene = kvStores.get("cyrene");



        athens.dropMessagesTo(byzantium);

        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Success", response);

        assertEquals("Microservices", athens.get("title").getValue());
        assertEquals("Microservices", cyrene.get("title").getValue());
        assertEquals("", byzantium.get("title").getValue());

        cyrene.dropMessagesTo(athens);
        cyrene.dropMessagesToAfter(byzantium, 1);
        //cyrene will read from itself and byzantium. byzantium has stale value, so it will try read-repair.
        String value = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Error", value);
        assertEquals("", byzantium.get("title").getValue());
    }

    @Test
    public void quorumsHaveIncompletelyWrittenValues() throws IOException {
        Map<String, QuorumKVStore> kvStores = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress,peerAddresses));

        QuorumKVStore athens = kvStores.get("athens");
        QuorumKVStore byzantium = kvStores.get("byzantium");
        QuorumKVStore cyrene = kvStores.get("cyrene");



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
        Map<String, QuorumKVStore> kvStores = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> {
                    config.setAsyncRepair();//TODO: refactor.
                    return new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress,peerAddresses);
                });

        QuorumKVStore athens = kvStores.get("athens");
        QuorumKVStore byzantium = kvStores.get("byzantium");
        QuorumKVStore cyrene = kvStores.get("cyrene");


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
        //Alice -   //Microservices:timestamp 2
                    //Nitroservices:timestamp 1
        String value = kvClient.getValue(athens.getClientConnectionAddress(), "title");
        assertEquals("Microservices", value);

        //concurrent read
        //there is a possibility of this happening.
        //Bob    //Nitroservices:timestamp 1
                 //Nitroservices:timestamp 1
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
        Map<String, QuorumKVStore> kvStores = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress,peerAddresses));

        QuorumKVStore athens = kvStores.get("athens");
        QuorumKVStore byzantium = kvStores.get("byzantium");
        QuorumKVStore cyrene = kvStores.get("cyrene");


        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        distrib.patterns.quorumconsensus.KVClient kvClient = new distrib.patterns.quorumconsensus.KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Nitroservices");
        assertEquals("Error", response);
        //quorum responses not received as messages to byzantium and cyrene fail.

        assertEquals("Nitroservices", athens.get("title").getValue());
        assertEquals(StoredValue.EMPTY, byzantium.get("title"));
        assertEquals(StoredValue.EMPTY, cyrene.get("title"));

        distrib.patterns.quorumconsensus.KVClient alice = new distrib.patterns.quorumconsensus.KVClient();

        //cyrene should be able to connect with itself and byzantium.
        //both cyrene and byzantium have empty value.
        //Alice starts the compareAndSwap
        //Alice reads the value.
        String aliceValue = alice.getValue(cyrene.getClientConnectionAddress(), "title");

        //meanwhile bob starts compareAndSwap as well
        //Bob connects to athens, which is now able to connect to cyrene and byzantium
        distrib.patterns.quorumconsensus.KVClient bob = new distrib.patterns.quorumconsensus.KVClient();
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
        assertEquals("Distributed Systems", response);
    }


    @Ignore //FIXME Modify handling of generations.
    @Test
    public void nodesShouldRejectRequestsFromPreviousGenerationNode() throws IOException {
        Map<String, QuorumKVStore> kvStores = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress,peerAddresses));

        QuorumKVStore primaryClusterNode = kvStores.get("athens");
        QuorumKVStore byzantium = kvStores.get("byzantium");
        QuorumKVStore cyrene = kvStores.get("cyrene");

        KVClient client = new KVClient();

        InetAddressAndPort oldPrimaryAddress = primaryClusterNode.getClientConnectionAddress();
        assertEquals("Success", client.setValue(oldPrimaryAddress, "key", "value"));

        assertEquals("value", client.getValue(oldPrimaryAddress, "key"));
        //Garbage collection pause...

        //Simulates starting a new primary instance because the first went under a GC pause.
        Config config = new Config(primaryClusterNode.getConfig().getWalDir().getAbsolutePath());
        InetAddressAndPort newClientAddress = TestUtils.randomLocalAddress();
        QuorumKVStore newInstance = new QuorumKVStore("athens1", config, new SystemClock(), newClientAddress, TestUtils.randomLocalAddress(), Arrays.asList(byzantium.getPeerConnectionAddress(), cyrene.getPeerConnectionAddress()));


        assertEquals(2, newInstance.getGeneration());
        String responseForNewWrite = client.setValue(newClientAddress, "key1", "value1");
        assertEquals("Success", responseForNewWrite);

        //Comes out of Garbage Collection pause.
        assertEquals("Rejecting request from generation 1 as already accepted from generation 2", client.setValue(oldPrimaryAddress, "key2", "value2"));
    }
}