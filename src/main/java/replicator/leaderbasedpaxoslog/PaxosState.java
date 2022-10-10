package replicator.leaderbasedpaxoslog;

import replicator.common.MonotonicId;
import replicator.wal.WALEntry;

import java.util.Optional;

public class PaxosState {
    MonotonicId promisedGeneration = MonotonicId.empty();
    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<WALEntry> acceptedValue = Optional.empty();

    Optional<WALEntry> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();
}
