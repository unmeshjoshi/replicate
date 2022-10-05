package distrib.patterns.paxoslog;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;
import distrib.patterns.wal.WALEntry;

public class ProposalRequest extends Request {
    public final MonotonicId generation;
    public final int index;
    public final WALEntry proposedValue;

    public ProposalRequest(MonotonicId generation, int index, WALEntry proposedValue) {
        super(RequestId.ProposeRequest);
        this.generation = generation;
        this.index = index;
        this.proposedValue = proposedValue;
    }
}
