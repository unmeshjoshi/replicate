package replicate.multipaxos;

import replicate.common.MonotonicId;

import java.util.Optional;

//no promised ballot for every entry.
public class PaxosState {
    Optional<MonotonicId> acceptedBallot = Optional.empty();
    Optional<byte[]> acceptedValue = Optional.empty();

    Optional<byte[]> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();
}
