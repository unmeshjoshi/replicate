package replicate.paxoslog;

import replicate.wal.WALEntry;

import java.util.Optional;

public class PaxosResult {
    public Optional<WALEntry> value;
    boolean success;

    public PaxosResult(Optional<WALEntry> value, boolean success) {
        this.value = value;
        this.success = success;
    }
}
