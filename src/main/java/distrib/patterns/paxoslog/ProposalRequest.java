package distrib.patterns.paxoslog;

import distrib.patterns.paxos.MonotonicId;
import distrib.patterns.wal.Command;
import distrib.patterns.wal.WALEntry;

public class ProposalRequest {
    private MonotonicId monotonicId;
    private int index;
    private WALEntry proposedValue;

    public ProposalRequest(MonotonicId monotonicId, int index, WALEntry proposedValue) {
        this.monotonicId = monotonicId;
        this.index = index;
        this.proposedValue = proposedValue;
    }

    public MonotonicId getMonotonicId() {
        return monotonicId;
    }

    public int getIndex() {
        return index;
    }

    public WALEntry getProposedValue() {
        return proposedValue;
    }
    //for jackson
    private ProposalRequest() {

    }
}
