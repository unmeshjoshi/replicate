package wal;

import common.TestUtils;
import distrib.patterns.common.Config;
import distrib.patterns.wal.DurableKVStore;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class DurableKVStoreTest {
    @Test
    public void shouldRecoverKVStoreStateFromWAL() {
        File walDir = TestUtils.tempDir("wal");
        DurableKVStore kv = new DurableKVStore(new Config(walDir.getAbsolutePath()));
        kv.put("key1", "value1");
        kv.put("key2", "value2");
        kv.put("key3", "value3");
        //KV crashes.
        kv.close();

        //simulates process restart. A new instance is created at startup.
        DurableKVStore recoveredKvStore = new DurableKVStore(new Config(walDir.getAbsolutePath()));

        assertEquals(recoveredKvStore.get("key1"), "value1");
        assertEquals(recoveredKvStore.get("key2"), "value2");
        assertEquals(recoveredKvStore.get("key3"), "value3");
    }


}