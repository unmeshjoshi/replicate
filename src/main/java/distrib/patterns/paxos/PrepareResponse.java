package distrib.patterns.paxos;

import distrib.patterns.common.MonotonicId;

import java.util.Optional;

public class PrepareResponse {
    boolean promised;
    public Optional<String> acceptedValue;
    public Optional<MonotonicId> acceptedGeneration;

    public PrepareResponse(boolean success, Optional<String> acceptedValue, Optional<MonotonicId> acceptedGeneration) {
        this.promised = success;
        this.acceptedValue = acceptedValue;
        this.acceptedGeneration = acceptedGeneration;
    }

    public PrepareResponse(boolean success) {
        this(success, Optional.empty(), Optional.empty());
    }

    //for jackson
    private PrepareResponse() {

    }

    public boolean isPromised() {
        return promised;
    }
}
