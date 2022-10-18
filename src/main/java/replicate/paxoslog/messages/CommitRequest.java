package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class CommitRequest extends Request {
    public final int index;
    public final byte[] committedValue;
    public final MonotonicId generation;

    public CommitRequest(int index, byte[] committedValue, MonotonicId generation) {
        super(RequestId.Commit);
        this.index = index;
        this.committedValue = committedValue;
        this.generation = generation;
    }
}
