package replicate.paxos;

import replicate.common.MonotonicId;

import java.util.Optional;

public record PaxosState(MonotonicId promisedBallot,
                         Optional<MonotonicId> acceptedBallot,
                         Optional<byte[]> acceptedValue,
                         Optional<byte[]> committedValue,
                         Optional<MonotonicId> committedGeneration){

    public PaxosState(){
        this(MonotonicId.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public boolean canAccept(MonotonicId generation) {
        return generation.equals(promisedBallot) || generation.isAfter(promisedBallot);
    }

    public PaxosState accept(MonotonicId generation, Optional<byte[]> value) {
        return new PaxosState(generation, Optional.of(generation), value, committedValue, committedGeneration);
    }

    public PaxosState promise(MonotonicId generation) {
        return new PaxosState(generation, acceptedBallot, acceptedValue, committedValue, committedGeneration);
    }

    public PaxosState commit(MonotonicId generation, Optional<byte[]> value) {
        return new PaxosState(generation, Optional.of(generation), value, value, Optional.of(generation));
    }

    public boolean canPromise(MonotonicId generation) {
        return generation.isAfter(this.promisedBallot);
    }
}