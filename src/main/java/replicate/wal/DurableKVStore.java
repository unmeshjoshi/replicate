package replicate.wal;

import replicate.common.Config;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DurableKVStore {
    //persistent..
    private final Map<String, String> kv = new HashMap<>();

    public String get(String key) {
        return kv.get(key);
    }

    public void put(String key, String value) {

        //Then applyLog at startup.
        //e.g. Cassandra's WAL for memtable.
        //pipeline
        //async
        //queue of requests |put() | put| put | | |-->
        // <--|resonse | put| put | | |
        //async arrayblockqueue
        appendLog(key, value);
        //async but preserver order
        kv.put(key, value);
        //async
        //respond to client
    }

    private Long appendLog(String key, String value) {
        Long aLong = wal.writeEntry(new SetValueCommand(key, value).serialize());
        return aLong;
    }

    //@VisibleForTesting
    final WriteAheadLog wal;
    private final Config config;

    public DurableKVStore(Config config) {
        this.config = config;
        this.wal = WriteAheadLog.openWAL(config);
        applyLog();
        //Assignment 1: applyLog at startup.
    }

    public void applyLog() {
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
    }

    private void applyEntries(List<WALEntry> walEntries) {
        for (WALEntry walEntry : walEntries) {
            Command command = deserialize(walEntry);
            if (command instanceof SetValueCommand) {
                SetValueCommand setValueCommand = (SetValueCommand) command;
                kv.put(setValueCommand.key, setValueCommand.value);
            }
        }
    }

    private Command deserialize(WALEntry walEntry) {
        return Command.deserialize(new ByteArrayInputStream(walEntry.getData()));
    }

    //simulates crash.
    public void close() {
        wal.close();
        kv.clear();
    }

    public Collection<String> values() {
        return kv.values();
    }
}
