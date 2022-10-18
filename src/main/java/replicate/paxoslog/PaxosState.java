package replicate.paxoslog;

import replicate.common.MonotonicId;

import java.util.Optional;

//TODO:Use PaxosState record
class PaxosState {
    MonotonicId promisedGeneration = MonotonicId.empty();
    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<byte[]> acceptedValue = Optional.empty();

    Optional<byte[]> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();
}
