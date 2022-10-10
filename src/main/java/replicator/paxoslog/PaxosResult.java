package replicator.paxoslog;

import replicator.wal.WALEntry;

import java.util.Optional;

public class PaxosResult {
    Optional<WALEntry> value;
    boolean success;

    public PaxosResult(Optional<WALEntry> value, boolean success) {
        this.value = value;
        this.success = success;
    }
}
