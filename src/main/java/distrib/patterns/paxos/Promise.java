package distrib.patterns.paxos;

import distrib.patterns.common.MonotonicId;

public class Promise {
    boolean accepted;

    MonotonicId acceptedGeneration;
    String acceptedValue;
}
