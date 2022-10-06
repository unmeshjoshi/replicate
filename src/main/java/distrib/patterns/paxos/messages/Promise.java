package distrib.patterns.paxos.messages;

import distrib.patterns.common.MonotonicId;

public class Promise {
    boolean accepted;

    MonotonicId acceptedGeneration;
    String acceptedValue;
}
