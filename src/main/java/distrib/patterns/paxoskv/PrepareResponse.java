package distrib.patterns.paxoskv;

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

    //for jackson
    private PrepareResponse() {

    }

}
