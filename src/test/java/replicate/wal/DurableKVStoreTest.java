package replicate.wal;

import org.junit.Test;
import replicate.common.Config;
import replicate.common.TestUtils;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class DurableKVStoreTest {
    @Test
    public void shouldRecoverKVStoreStateFromWAL() {
        //public static void main(String args[]) {
        File walDir = TestUtils.tempDir("distrib/patterns/wal");
        DurableKVStore kv = new DurableKVStore(new Config(walDir.getAbsolutePath()));
        kv.put("title", "Microservices");
        //client got success;
        //client is sure that key1 is saved
        //fail.
        //success
        kv.put("author", "Martin");
        //client is sure that key2 is saved
        kv.put("newTitle", "Distributed Systems");
        //client is sure that key3 is saved
        //}

        //KV crashes.
        kv.close();

        //simulates process restart. A new instance is created at startup.
        DurableKVStore recoveredKvStore = new DurableKVStore(new Config(walDir.getAbsolutePath()));

        assertEquals(recoveredKvStore.get("title"), "Microservices");
        assertEquals(recoveredKvStore.get("author"), "Martin");
        assertEquals(recoveredKvStore.get("newTitle"), "Distributed Systems");
    }


}