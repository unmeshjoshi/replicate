package replicate.paxos.messages;

import replicate.common.MonotonicId;

public class Promise {
    boolean accepted;
    MonotonicId acceptedGeneration;
    String acceptedValue;
}
