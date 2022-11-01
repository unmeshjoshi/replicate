package replicate.paxoslog;

import replicate.wal.WALEntry;

import java.util.Optional;

public class PaxosResult {
    public Optional<byte[]> value;
    boolean success;

    public PaxosResult(Optional<byte[]> value, boolean success) {
        this.value = value;
        this.success = success;
    }
}
