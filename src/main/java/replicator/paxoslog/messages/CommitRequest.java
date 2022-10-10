package replicator.paxoslog.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;
import replicator.wal.WALEntry;

public class CommitRequest extends Request {
    public final int index;
    public final WALEntry proposedValue;
    public final MonotonicId generation;

    public CommitRequest(int index, WALEntry committedValue, MonotonicId generation) {
        super(RequestId.Commit);
        this.index = index;
        this.proposedValue = committedValue;
        this.generation = generation;
    }
}
