package replicator.paxos.messages;

import replicator.common.MonotonicId;

public class Promise {
    boolean accepted;
    MonotonicId acceptedGeneration;
    String acceptedValue;
}
