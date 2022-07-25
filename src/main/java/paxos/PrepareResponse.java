package paxos;

import net.requestwaitinglist.MonotonicId;
import net.requestwaitinglist.SingleValuePaxosClusterNode;

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
