package replicator.wal;

import replicator.common.TestUtils;
import replicator.common.Config;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class DurableKVStoreTest {
    @Test
    public void shouldRecoverKVStoreStateFromWAL() {
        //public static void main(String args[]) {
        File walDir = TestUtils.tempDir("distrib/patterns/wal");
        DurableKVStore kv = new DurableKVStore(new Config(walDir.getAbsolutePath()));
        kv.put("key1", "value1");
        //client got success;
        //client is sure that key1 is durable
        //fail.
        //success
        kv.put("key2", "value2");
        //client is sure that key2 is durable
        kv.put("key3", "value3");
        //client is sure that key3 is durable
        //}

        //KV crashes.
        kv.close();

        //simulates process restart. A new instance is created at startup.
        DurableKVStore recoveredKvStore = new DurableKVStore(new Config(walDir.getAbsolutePath()));

        assertEquals(recoveredKvStore.get("key1"), "value1");
        assertEquals(recoveredKvStore.get("key2"), "value2");
        assertEquals(recoveredKvStore.get("key3"), "value3");
    }


}