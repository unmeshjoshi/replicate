package distrib.patterns.paxoslog;

import distrib.patterns.paxos.MonotonicId;
import distrib.patterns.wal.Command;
import distrib.patterns.wal.WALEntry;

public class CommitRequest {
    private int index;
    private WALEntry proposedValue;
    private MonotonicId monotonicId;

    public CommitRequest(int index, WALEntry committedValue, MonotonicId monotonicId) {
        this.index = index;
        this.proposedValue = committedValue;
        this.monotonicId = monotonicId;
    }

    public MonotonicId getMonotonicId() {
        return monotonicId;
    }

    public WALEntry getProposedValue() {
        return proposedValue;
    }

    public int getIndex() {
        return index;
    }

    //for jackson
    private CommitRequest(){}
}
