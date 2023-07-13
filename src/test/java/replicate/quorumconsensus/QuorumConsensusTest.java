package replicate.quorumconsensus;

import org.junit.Assert;
import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.MonotonicId;
import replicate.common.TestUtils;
import replicate.net.requestwaitinglist.TestClock;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuorumConsensusTest extends ClusterTest<QuorumConsensus> {

    @Test
    public void readRepair() throws IOException {
        this.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumConsensus(name, config, clock, clientConnectionAddress, peerConnectionAddress, true, peerAddresses));
        QuorumConsensus athens = nodes.get("athens");
        QuorumConsensus byzantium = nodes.get("byzantium");
        QuorumConsensus cyrene = nodes.get("cyrene");


        athens.dropMessagesTo(byzantium);

        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Success", response);


        MonotonicId id1 = athens.getVersion("title");
        MonotonicId id2 = byzantium.getVersion("title");
        MonotonicId id3 = cyrene.getVersion("title");

        assertEquals(id1, new MonotonicId(1, 0));
        assertEquals(MonotonicId.empty(), id2);
        assertEquals(id3, new MonotonicId(1, 0));

        String title = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertEquals("Microservices", title);

        Assert.assertEquals(new MonotonicId(1, 0), byzantium.getVersion("title"));
    }

    @Test
    public void laterReadsNeverGetOlderValue() throws IOException {
        this.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumConsensus(name, config, clock, clientConnectionAddress, peerConnectionAddress, true, peerAddresses));
        var athens = nodes.get("athens");
        var byzantium = nodes.get("byzantium");
        var cyrene = nodes.get("cyrene");

        KVClient kvClient = new KVClient();
        //drop messages only after first message. The first getVersion message will succeed on all the nodes.
        athens.dropAfterNMessagesTo(cyrene, 1); //byzantium wont have this value.
        athens.dropAfterNMessagesTo(byzantium, 1); //cyrene wont have this value.

        //Nathan
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Nicroservices");
        assertEquals("Error", response);

        assertEquals("Nicroservices", athens.get("title").getValue());
        assertEquals("", cyrene.get("title").getValue());
        assertEquals("", byzantium.get("title").getValue());


        //cyrene can not talk to athens
        cyrene.dropMessagesTo(athens);

        //Philip
        response = kvClient.setValue(cyrene.getClientConnectionAddress(), "title", "Microservices");
        assertEquals("Success", response);

        assertEquals("Microservices", cyrene.get("title").getValue());
        assertEquals("Microservices", byzantium.get("title").getValue());
        assertEquals("Nicroservices", athens.get("title").getValue());

        //problem solved. Older value wont have higher timestamp.
        assertTrue(athens.get("title").getVersion().isBefore(cyrene.get("title").getVersion()));
        assertTrue(byzantium.get("title").getVersion().equals(cyrene.get("title").getVersion()));

        //all connections now restored.
        athens.reconnectTo(cyrene);
        cyrene.reconnectTo(athens);


        //Alice -   //Microservices:timestamp 1 athens
        //Nitroservices:timestamp 2 cyrene
        //triggers read-repair..
        String value = kvClient.getValue(athens.getClientConnectionAddress(), "title");
        assertEquals("Microservices", value);
        assertEquals("Microservices", cyrene.get("title").getValue());


        //read a value for title == "microrservices" //which is the latest value. connecting to athens
        //after some time i again read title, connecting to byzantium. and I get old value.

    }

    //
    @Test
    public void olderIncompleteValueAtHigherRankCanOverwriteNewerValue() throws IOException {
        //Three node cluster,
        // athens serverId = 0, byzantium serverId = 1, cyrene serverId = 2
        this.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumConsensus(name, config, clock, clientConnectionAddress, peerConnectionAddress, true, peerAddresses));

        var athens = nodes.get("athens");
        var byzantium = nodes.get("byzantium");
        var cyrene = nodes.get("cyrene");


        /**
         *cyrene can read the versions, but fails to write to any of the nodes.
         * So the write for key title, value Nicroservices, fails. But there
         * is an incomplete write on cyrene itself with a version (1, 2)
         */

        KVClient kvClient = new KVClient();
        //drop messages only after first message. The first getVersion message will succeed on all the nodes.
        cyrene.dropAfterNMessagesTo(athens, 1); //byzantium wont have this
        // value.
        cyrene.dropAfterNMessagesTo(byzantium, 1); //cyrene wont have this
        // value.

        //Nathan
        String response =
                kvClient.setValue(cyrene.getClientConnectionAddress(), "title", "Nicroservices");
        assertEquals("Error", response);

        MonotonicId id1 = athens.getVersion("title");
        MonotonicId id2 = byzantium.getVersion("title");
        MonotonicId id3 = cyrene.getVersion("title");

        assertEquals(id1, MonotonicId.empty());
        assertEquals(id2, MonotonicId.empty());
        assertEquals(id3, new MonotonicId(1, 2));

        //2. Another user
        String response2 =
                kvClient.setValue(athens.getClientConnectionAddress(), "title",
                        "Distributed Systems");

        assertEquals("Success", response2);
        assertEquals(athens.getVersion("title"), new MonotonicId(1, 0));
        assertEquals(byzantium.getVersion("title"), new MonotonicId(1, 0));
        assertEquals(cyrene.getVersion("title"),new MonotonicId(1, 2));


        //3. All nodes reconnect. But now a subsequent read will return the old
        //   incomplete value.
        //cyrene connects to byzantium, now able to fulfill majority quorum
        // for reads and writes.

        // value.
        cyrene.reconnectTo(byzantium);

        String value = kvClient.getValue(cyrene.getClientConnectionAddress(),
                "title");
        assertEquals(value, "Nicroservices");

        //all nodes now have a value from previous incomplete write.
        assertEquals(athens.get("title").getValue(), "Distributed Systems");
        assertEquals(byzantium.get("title").getValue(), "Nicroservices");
        assertEquals(cyrene.get("title").getValue(),"Nicroservices");

    }

    @Test
    public void compareAndSwapIsSuccessfulForTwoConcurrentClients() throws IOException {
        Map<String, QuorumConsensus> kvStores = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumConsensus(name, config, clock, clientConnectionAddress, peerConnectionAddress, true, peerAddresses));
        QuorumConsensus athens = kvStores.get("athens");
        QuorumConsensus byzantium = kvStores.get("byzantium");
        QuorumConsensus cyrene = kvStores.get("cyrene");

        athens.dropAfterNMessagesTo(byzantium, 1);
        athens.dropMessagesTo(cyrene);

        KVClient kvClient = new KVClient();
        String response = kvClient.setValue(athens.getClientConnectionAddress(), "title", "Nitroservices");
        assertEquals("Error", response);
        //quorum responses not received as messages to byzantium and cyrene fail.

        Assert.assertEquals(new MonotonicId(1, 0), athens.getVersion("title"));
        Assert.assertEquals(new MonotonicId(-1, -1), byzantium.getVersion("title"));
        Assert.assertEquals(new MonotonicId(-1, -1), cyrene.getVersion("title"));

        KVClient alice = new KVClient();

        //cyrene should be able to connect with itself and byzantium.
        //both cyrene and byzantium have empty value.
        //Alice starts the compareAndSwap
        //Alice reads the value.
        String aliceValue = alice.getValue(cyrene.getClientConnectionAddress(), "title");

        //meanwhile bob starts compareAndSwap as well
        //Bob connects to athens, which is now able to connect to cyrene and byzantium
        KVClient bob = new KVClient();
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