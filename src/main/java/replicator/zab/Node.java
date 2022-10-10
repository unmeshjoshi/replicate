package replicator.zab;

import replicator.wal.Command;
import replicator.wal.SetValueCommand;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

public class Node {
    Map<Long, LogEntry> log = new HashMap<>();
    long lastLoggedZxid;
    long currentEpoch;
    long acceptedEpoch;

    Map<String, String> kv = new HashMap<>();
    protected void commit(LogEntry logEntry) {
        Command command = Command.deserialize(new ByteArrayInputStream(logEntry.command));
        if (command instanceof SetValueCommand) {
            SetValueCommand setValueCommand = (SetValueCommand)command;
            kv.put(setValueCommand.getKey(), setValueCommand.getValue());
        }
    }

    public static long getEpochFromZxid(long zxid) {
        return zxid >> 32L;
    }
    public static long getCounterFromZxid(long zxid) {
        return zxid & 0xffffffffL;
    }
    public static long makeZxid(long epoch, long counter) {
        return (epoch << 32L) | (counter & 0xffffffffL);
    }
    public static String zxidToString(long zxid) {
        return Long.toHexString(zxid);
    }
}
