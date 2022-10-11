package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;
import replicate.wal.WALEntry;

public class CommitRequest extends Request {
    public final int index;
    public final WALEntry committedValue;
    public final MonotonicId generation;

    public CommitRequest(int index, WALEntry committedValue, MonotonicId generation) {
        super(RequestId.Commit);
        this.index = index;
        this.committedValue = committedValue;
        this.generation = generation;
    }
}
