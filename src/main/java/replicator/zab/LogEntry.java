package replicator.zab;

import replicator.wal.Command;

public class LogEntry {
    long zxid;
    byte[] command;

    public LogEntry(long zxid, Command command) {
        this.zxid = zxid;
        this.command = command.serialize();
    }
}
