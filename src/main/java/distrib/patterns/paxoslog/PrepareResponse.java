package distrib.patterns.paxoslog;

import distrib.patterns.paxos.MonotonicId;
import distrib.patterns.wal.Command;
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
