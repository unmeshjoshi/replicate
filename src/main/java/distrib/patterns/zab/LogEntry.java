package distrib.patterns.zab;

import distrib.patterns.wal.Command;

public class LogEntry {
    long zxid;
    byte[] command;

    public LogEntry(long zxid, Command command) {
        this.zxid = zxid;
        this.command = command.serialize();
    }
}
