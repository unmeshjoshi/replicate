package replicate.paxos;

import replicate.common.MonotonicId;

import java.util.Optional;

public record PaxosState(
        //Generation value to avoid concurrent updates
        //Only the node with the highest generation will be
        // able to coordinate replication. Can be called as
        // 'the leader'. Because of failures, or message
        // delays,if majority quorum has is a new leader
        // with higher generation, all the requests from
        // previous generation will be rejected.
        MonotonicId promisedGeneration,
        //The Generation value for the accepted requests.
        //Because of failures, various cluster nodes can end
        //up having accepted different values.
        //For repairing the cluster, the new leader
        // will always choose the highest generation value
        //if it receives different accepted values from
        //different cluster nodes.
        Optional<MonotonicId> acceptedGeneration,
        Optional<byte[]> acceptedValue,

        Optional<byte[]> committedValue,
        Optional<MonotonicId> committedGeneration) {

    public PaxosState() {
        this(MonotonicId.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public boolean canAccept(MonotonicId generation) {
        return generation.equals(promisedGeneration) || generation.isAfter(promisedGeneration);
    }

    public PaxosState accept(MonotonicId generation, Optional<byte[]> value) {
        return new PaxosState(generation, Optional.of(generation), value, committedValue, committedGeneration);
    }

    public PaxosState promise(MonotonicId generation) {
        return new PaxosState(generation, acceptedGeneration, acceptedValue, committedValue, committedGeneration);
    }

    public PaxosState commit(MonotonicId generation, Optional<byte[]> value) {
        return new PaxosState(generation, Optional.of(generation), value, value, Optional.of(generation));
    }

    public boolean canPromise(MonotonicId generation) {
        return generation.isAfter(this.promisedGeneration);
    }
}