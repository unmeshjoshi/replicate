package replicate.quorum;

import org.junit.Test;
import replicate.common.ClusterTest;
import replicate.common.MessageId;
import replicate.common.NetworkClient;
import replicate.common.TestUtils;
import replicate.net.InetAddressAndPort;
import replicate.net.requestwaitinglist.TestClock;
import replicate.quorum.messages.GetValueResponse;
import replicate.quorum.messages.SetValueResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuorumKVStoreTest extends ClusterTest<QuorumKVStore> {
    QuorumKVStore athens;
    QuorumKVStore byzantium;
    QuorumKVStore cyrene;

    @Override
    public void setUp() throws IOException {
        this.nodes = TestUtils.startCluster(Arrays.asList("athens", "byzantium", "cyrene"),
                (name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses) -> new QuorumKVStore(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses));

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
        var setValueResponse = kvClient.setValue(athensAddress, "title",
                "Microservices");
        assertResponseSuccess(setValueResponse);

        //how to make sure replicas are in sync?

        var getValueResponse = kvClient.getValue(athensAddress, "title");

        assertResponseValue(getValueResponse, "Microservices");

        QuorumKVStore[] nodes = {athens, byzantium, cyrene};
        String[] expectedTitles = {"Microservices", "", "Microservices"};
        assertTitleValues(nodes, expectedTitles);
    }


    @Test
    public void quorumReadRepairUpdatesStaleValues() throws IOException {
        //setup initial value.
        KVClient kvClient = new KVClient();
        var initialResponse =
                kvClient.setValue(athens.getClientConnectionAddress(),
                        "title", "Initial title");
        assertResponseSuccess(initialResponse);

        athens.dropMessagesTo(byzantium);

        var updatedResponse =
                kvClient.setValue(athens.getClientConnectionAddress(), "title", "Updated title");
        assertResponseSuccess(updatedResponse);

        QuorumKVStore[] nodes = {athens, byzantium, cyrene};
        String[] initialExpectedTitles
                = {"Updated title", "Initial title", "Updated title"};
        assertTitleValues(nodes, initialExpectedTitles);

        //response from cyrene and byzantium.
        var titleResponse =
                kvClient.getValue(cyrene.getClientConnectionAddress(),
                        "title");
        assertResponseValue(titleResponse, "Updated title");

        String[] expectedTitles
                = {"Updated title", "Updated title", "Updated title"};
        assertTitleValues(nodes, expectedTitles);
    }

    private static String valueFrom(NetworkClient.Response<GetValueResponse> titleResponse) {
        return titleResponse.getResult().getValue().getValue();
    }


    @Test
    public void quorumReadFailsWhenSynchronousReadRepairFails() throws IOException {

        athens.dropMessagesTo(byzantium);

        KVClient kvClient = new KVClient();
        var setValueResponse = kvClient.setValue(athens.getClientConnectionAddress(),
                "title", "Microservices");
        assertResponseSuccess(setValueResponse);

        assertTitleEquals(athens, "Microservices");
        assertTitleEquals(cyrene, "Microservices");
        assertTitleEquals(byzantium, "");

        cyrene.dropMessagesTo(athens);
        cyrene.dropAfterNMessagesTo(byzantium, 1);
        //cyrene will read from itself and byzantium. byzantium has stale value, so it will try read-repair.
        //but read-repair call fails.
        var titleResponse = kvClient.getValue(cyrene.getClientConnectionAddress(),
                "title");
        assertResponseFailure(titleResponse);
        assertTitleEquals(byzantium, "");
    }

    private void assertTitleValues(QuorumKVStore[] nodes, String[] expectedTitles) {
        assertEquals(nodes.length, expectedTitles.length);

        for (int i = 0; i < nodes.length; i++) {
            assertEquals(expectedTitles[i], nodes[i].get("title").getValue());
        }
    }

    private void assertTitleEquals(QuorumKVStore quorumKVStore, String microservices) {
        assertEquals(microservices, quorumKVStore.get("title").getValue());
    }

    private static void assertResponseSuccess(NetworkClient.Response<?> response) {
        assertTrue(response.isSuccess());
    }

    private static void assertResponseFailure(NetworkClient.Response<?> response) {
        assertTrue(response.isError());
    }

    @Test
    public void quorumReadCanGetIncompletelyWrittenValues() throws IOException {
        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        KVClient kvClient = new KVClient();
        var setValueResponse =
                kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertResponseFailure(setValueResponse);
        assertTitleEquals(athens, "Microservices");

        athens.reconnectTo(cyrene);
        cyrene.dropMessagesTo(byzantium);

        var titleResponse =
                kvClient.getValue(cyrene.getClientConnectionAddress(),
                        "title");
        assertResponseValue(titleResponse, "Microservices");

    }


    @Test
    public void readsConcurrentWithWriteCanGetOldValueBecauseOfMessageDelays() throws IOException, InterruptedException, ExecutionException {
        KVClient kvClient = new KVClient();
        var setValueResponse =
                kvClient.setValue(athens.getClientConnectionAddress(),
                        "title", "Initial Value");

        assertResponseSuccess(setValueResponse);

        assertTitleEquals(athens, "Initial Value");

        athens.addDelayForMessagesOfType(byzantium, MessageId.VersionedSetValueRequest);
        athens.addDelayForMessagesOfType(cyrene,
                MessageId.VersionedSetValueRequest);
        athens.addDelayForMessagesOfType(cyrene,
                MessageId.GetValueResponse);


        //This is an approximate test, only for demonstration. Hoping that
        // setValue is scheduled ahead
        //of getValue request. If its scheduled later, the get will definitely
        //get the Initial Value. But even if setValue is scheduled earlier,
        // which most likely be the case, because the replication is delayed,
        //the get request going to different nodes will get different values.
        //If the get request is originated at cyrene, it will always get the
        //initial value. If the get request goes to athens, it will get the
        //updated value or initial value, depending on whether the set value
        //is scheduled earlier.
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        var setValueF = executorService.submit(() -> {
            try {
                return kvClient.setValue(athens.getClientConnectionAddress(), "title", "Updated Value");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        var getValueF = executorService.submit(() -> {
            try {
                return kvClient.getValue(cyrene.getClientConnectionAddress(),
                        "title");


            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertResponseSuccess(setValueF.get());
        assertResponseValue(getValueF.get(), "Initial Value");

    }

    //With async read-repair, a client reading after another client can see older values if two different clients
    //read from two different servers.
    @Test
    public void withAsyncReadRepairlaterReadsCanGetOlderValue() throws IOException {
        athens.doAsyncReadRepair();

        KVClient kvClient = new KVClient();
        //Nathan
        var initialResponse =
                kvClient.setValue(athens.getClientConnectionAddress(),
                        "title", "Nicroservices");
        assertResponseSuccess(initialResponse);

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        //Philip
        var secondResponse =
                kvClient.setValue(athens.getClientConnectionAddress(), "title", "Microservices");
        assertResponseFailure(secondResponse);


        assertTitleEquals(athens, "Microservices");
        assertTitleEquals(byzantium, "Nicroservices");
        assertTitleEquals(cyrene, "Nicroservices");

        athens.reconnectTo(cyrene);
        athens.addDelayForMessagesToAfterNMessages(cyrene, 1);

        //concurrent read
        //read-repair message to cyrene is delayed..
        //Alice -   //Microservices:timestamp 2 athens
        //Nitroservices:timestamp 1 cyrene
        //triggers read-repair
        // return "Microservices"
        // GC Pause on athens
        // as part of get request --ReadRepair(SetValue title, "Microservices", ts=2)->cyrene //happens async and delayed.
        var firstTitleResponse =
                kvClient.getValue(athens.getClientConnectionAddress(),
                        "title");
        assertResponseValue(firstTitleResponse, "Microservices");


        //concurrent read
        //there is a possibility of this happening.
        //Bob is reading after Alice. But still Bob gets older value.
        //Bob    //Nitroservices:timestamp 1 byzantium
        //Nitroservices:timestamp 1 cyrene
        var secondTitleResponse =
                kvClient.getValue(byzantium.getClientConnectionAddress(), "title");
        assertResponseValue(secondTitleResponse, "Nicroservices");

        TestUtils.waitUntilTrue(() -> {
            try {
                return "Microservices".equals(valueFrom(kvClient.getValue(byzantium.getClientConnectionAddress(), "title")));
            } catch (IOException e) {
                return false;
            }
        }, "Waiting for read-repair to complete", Duration.ofSeconds(5));
    }

    private static void assertResponseValue(NetworkClient.Response<GetValueResponse> getValueResponse, String expectedValue) {
        assertEquals(expectedValue, valueFrom(getValueResponse));
    }

    //Even with sync read-repair, a client reading after another client can see older values.
    @Test
    public void laterReadsGetOlderIncompletelyWrittenValueBecauseOfClockSkew() throws IOException {
        KVClient kvClient = new KVClient();
        athens.dropMessagesTo(cyrene); //byzantium wont have this value, but quorum is reached.
        athens.dropMessagesTo(byzantium); //cyrene wont have this value, but quorum is reached.
        athens.setClock(new TestClock(200));//athens clock is ahead of
        // everyone else.    title=>Initial Value, 200
                        //write title=>Updated Value, 100

        byzantium.setClock(new TestClock(200));
        cyrene.setClock(new TestClock(100));

        var aliceResponse =
                kvClient.setValue(athens.getClientConnectionAddress(),
                        "title", "Nicroservices");
        assertResponseFailure(aliceResponse);

        assertTitleEquals(athens, "Nicroservices");
        assertTitleEquals(cyrene, "");
        assertTitleEquals(byzantium, "");


        //cyrene can not talk to athens
        cyrene.dropMessagesTo(athens);

        //cyrene's clock is lagging at 100. So cyrene will set this value, but byzantium will silently drop it. athens is
        //reached, so it will have old value, at timestamp 200.
        //Philip
        var bobResponse =
                kvClient.setValue(cyrene.getClientConnectionAddress(), "title", "Microservices");
        assertResponseSuccess(bobResponse);
        //cyrene now gets a value which is fixed, but at a lower timestamp.

        assertTitleEquals(cyrene, "Microservices");
        assertTitleEquals(byzantium, "Microservices");
        assertTitleEquals(athens, "Nicroservices");

        //problem. Older value has higher timestamp.
        assertTrue(athens.get("title").getTimestamp() > cyrene.get("title").getTimestamp());
        assertTrue(byzantium.get("title").getTimestamp() == cyrene.get("title").getTimestamp());

        //all connections now restored.
        athens.reconnectTo(cyrene);
        cyrene.reconnectTo(athens);


        //Alice -   //Microservices:timestamp 1 athens
        //Nitroservices:timestamp 2 cyrene
        //triggers read-repair.. but... because older value has higher timestamp. We get the latest value overwritten.
        // return "Nitroservices"
        var titleResponse =
                kvClient.getValue(athens.getClientConnectionAddress(),
                        "title");
        assertResponseValue(titleResponse, "Nicroservices");
        assertTitleEquals(cyrene, "Nicroservices");

    }


    //Even with sync read-repair, a client reading after another client can see older values.
    @Test
    public void laterReadsGetOlderValueBecauseOfClockSkew() throws IOException {
        KVClient kvClient = new KVClient();
        athens.dropMessagesTo(cyrene); //cyrene does not have this value, but quorum is reached.
        athens.setClock(new TestClock(200));
        //Nathan
        var firstSetValueResponse =
                kvClient.setValue(athens.getClientConnectionAddress(), "title", "Nicroservices");
        assertResponseSuccess(firstSetValueResponse);


        cyrene.setClock(new TestClock(100));

        //Question::how to make sure there is no clock skew?
        cyrene.dropMessagesTo(athens);

        //cyrene's clock is lagging at 100. So cyrene will set this value, but byzantium will silently drop it. athens is
        //reached, so it will have old value, at timestamp 200.
        //Philip
        var secondSetValueResponse =
                kvClient.setValue(cyrene.getClientConnectionAddress(), "title", "Microservices");
        assertResponseSuccess(secondSetValueResponse);
        //cyrene now gets a value which is fixed, but at a lower timestamp.

        assertTitleEquals(cyrene, "Microservices");
        assertTitleEquals(byzantium, "Nicroservices");
        assertTitleEquals(athens, "Nicroservices");

        //problem. Older value has higher timestamp.
        assertTrue("Nicroservices", athens.get("title").getTimestamp() > cyrene.get("title").getTimestamp());
        assertTrue("Nicroservices", byzantium.get("title").getTimestamp() > cyrene.get("title").getTimestamp());

        //all connections now restored.
        athens.reconnectTo(cyrene);
        cyrene.reconnectTo(athens);

        athens.dropMessagesTo(byzantium);

        //Alice -   //Microservices:timestamp 100 athens
        //Nitroservices:timestamp 200 cyrene
        //triggers read-repair.. but... because older value has higher timestamp. We get the latest value overwritten.
        // return "Nitroservices"
        var titleResponse =
                kvClient.getValue(athens.getClientConnectionAddress(),
                "title");
        assertResponseValue(titleResponse, "Nicroservices");
        assertTitleEquals(cyrene, "Nicroservices");

    }

    //Incomplete write requests cause two different clients to see different values
    //depending on which nodes they connect to.
    @Test
    public void compareAndSwapIsSuccessfulForTwoConcurrentClients() throws IOException {

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        KVClient kvClient = new KVClient();
        var setValueResponse =
                kvClient.setValue(athens.getClientConnectionAddress(),
                "title", "Nitroservices");
        assertResponseFailure(setValueResponse);
        //quorum responses not received as messages to byzantium and cyrene fail.

        assertTitleEquals(athens, "Nitroservices");
        assertEquals(StoredValue.EMPTY, byzantium.get("title"));
        assertEquals(StoredValue.EMPTY, cyrene.get("title"));

        KVClient alice = new KVClient();

        //cyrene should be able to connect with itself and byzantium.
        //both cyrene and byzantium have empty value.
        //Alice starts the compareAndSwap
        //Alice reads the value.
        var firstTitleResponse =
                alice.getValue(cyrene.getClientConnectionAddress(),
                "title");
        //get-compare-modify-write
        //meanwhile bob starts compareAndSwap as well
        //Bob connects to athens, which is now able to connect to cyrene and byzantium
        KVClient bob = new KVClient();
        athens.reconnectTo(cyrene);
        athens.reconnectTo(byzantium);

        //this whole operation as a command.
        //send this whole operation for servers to execute.
        // read a value
        //check the value for some condition
        //set a value.
        var secondTitleResponse =
                bob.getValue(athens.getClientConnectionAddress(),
                "title");
        if (valueFrom(secondTitleResponse).equals("Nitroservices")) {
            kvClient.setValue(athens.getClientConnectionAddress(), "title", "Distributed Systems");
        }
//        commit;

        //Bob successfully completes compareAndSwap

        //Alice checks the value to be empty.
        if (valueFrom(firstTitleResponse).equals("")) {
            alice.setValue(cyrene.getClientConnectionAddress(), "title", "Nitroservices");
        }
        //Alice successfully completes compareAndSwap

        //Bob is surprised to read the different value after his compareAndSwap was successful.
        var thirdTitleResponse =
                bob.getValue(cyrene.getClientConnectionAddress(), "title");
        assertResponseValue(thirdTitleResponse, "Nitroservices");
    }
}