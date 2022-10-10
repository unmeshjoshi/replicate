package replicator.paxoslog.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;
import replicator.wal.WALEntry;

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
