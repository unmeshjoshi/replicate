package distrib.patterns.paxoslog;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.wal.WALEntry;

import java.util.Optional;

public class PrepareResponse {
    boolean promised;
    public Optional<WALEntry> acceptedValue;
    public Optional<MonotonicId> acceptedGeneration;

    public PrepareResponse(boolean success, Optional<WALEntry> acceptedValue, Optional<MonotonicId> acceptedGeneration) {
        this.promised = success;
        this.acceptedValue = acceptedValue;
        this.acceptedGeneration = acceptedGeneration;
    }

    //for jackson
    private PrepareResponse() {

    }

}
