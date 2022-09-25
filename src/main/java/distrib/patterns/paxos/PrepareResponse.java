package distrib.patterns.paxos;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

import java.util.Optional;

public class PrepareResponse extends Request {
    boolean promised;
    public Optional<String> acceptedValue;
    public Optional<MonotonicId> acceptedGeneration;

    public PrepareResponse(boolean success, Optional<String> acceptedValue, Optional<MonotonicId> acceptedGeneration) {
        super(RequestId.Promise);
        this.promised = success;
        this.acceptedValue = acceptedValue;
        this.acceptedGeneration = acceptedGeneration;
    }

    public PrepareResponse(boolean success) {
        this(success, Optional.empty(), Optional.empty());
    }

    //for jackson
    private PrepareResponse() {
        super(RequestId.Promise);
    }

    public boolean isPromised() {
        return promised;
    }
}
