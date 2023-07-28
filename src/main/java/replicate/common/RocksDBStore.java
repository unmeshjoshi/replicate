package replicate.common;

import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class RocksDBStore {
    public void put(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    //<codeFragment name="rocksdbstorage">
    private final RocksDB db;

    public RocksDBStore(File cacheDir) {
        Options options = new Options();
        options.setKeepLogFileNum(30);
        options.setCreateIfMissing(true);
        options.setLogFileTimeToRoll(TimeUnit.DAYS.toSeconds(1));
        try {
            db = RocksDB.open(options, cacheDir.getPath());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        db.close();
    }
}
