package paxos;

import net.requestwaitinglist.MonotonicId;

public class Promise {
    boolean accepted;

    MonotonicId acceptedGeneration;
    String acceptedValue;
}
