package distrib.patterns.paxoslog;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.wal.WALEntry;

import java.util.Optional;

//TODO:Refactor paxos state class to be immutable.
class PaxosState {
    MonotonicId promisedGeneration = MonotonicId.empty();
    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<WALEntry> acceptedValue = Optional.empty();

    Optional<WALEntry> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();
}
