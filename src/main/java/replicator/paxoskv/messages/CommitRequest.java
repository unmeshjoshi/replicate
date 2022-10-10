package replicator.paxoskv.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;

public class CommitRequest extends Request {
    public final String key;
    public final String value;
    public final MonotonicId generation;

    public CommitRequest(String key, String value, MonotonicId generation) {
        super(RequestId.Commit);
        this.key = key;
        this.value = value;
        this.generation = generation;
    }
}
