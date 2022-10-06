package distrib.patterns.paxoslog.messages;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;
import distrib.patterns.wal.WALEntry;

public class CommitRequest extends Request {
    public final int index;
    public final WALEntry proposedValue;
    public final MonotonicId monotonicId;

    public CommitRequest(int index, WALEntry committedValue, MonotonicId monotonicId) {
        super(RequestId.Commit);
        this.index = index;
        this.proposedValue = committedValue;
        this.monotonicId = monotonicId;
    }
}
