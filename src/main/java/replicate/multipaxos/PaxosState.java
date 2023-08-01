package replicate.multipaxos;

import replicate.common.MonotonicId;

import java.util.Optional;

public record PaxosState(Optional<MonotonicId> acceptedGeneration,
                         Optional<byte[]> acceptedValue,
                         Optional<byte[]> committedValue,
                         Optional<MonotonicId> committedGeneration){

    public PaxosState(){
        this(Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public PaxosState accept(MonotonicId generation, Optional<byte[]> value) {
        return new PaxosState(Optional.of(generation), value, committedValue, committedGeneration);
    }

    public PaxosState commit(MonotonicId generation, Optional<byte[]> value) {
        return new PaxosState(Optional.of(generation), value, value, Optional.of(generation));
    }
}