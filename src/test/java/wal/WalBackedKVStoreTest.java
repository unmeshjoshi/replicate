package wal;

import common.TestUtils;
import distrib.patterns.common.Config;
import distrib.patterns.wal.WalBackedKVStore;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class WalBackedKVStoreTest {
    @Test
    public void shouldRecoverKVStoreStateFromWAL() {
        File walDir = TestUtils.tempDir("wal");
        WalBackedKVStore kv = new WalBackedKVStore(new Config(walDir.getAbsolutePath()));
        kv.put("key1", "value1");
        kv.put("key2", "value2");
        kv.put("key3", "value3");
        kv.close();

        //simulates process restart. A new instance is created at startup.
        WalBackedKVStore recoveredKvStore = new WalBackedKVStore(new Config(walDir.getAbsolutePath()));

        assertEquals(recoveredKvStore.get("key1"), "value1");
        assertEquals(recoveredKvStore.get("key2"), "value2");
        assertEquals(recoveredKvStore.get("key3"), "value3");
    }


}