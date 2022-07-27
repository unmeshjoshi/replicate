package distrib.patterns.paxos;

public class Promise {
    boolean accepted;

    MonotonicId acceptedGeneration;
    String acceptedValue;
}
