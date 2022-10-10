package replicator.paxoskv.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;

public class PrepareRequest extends Request {
    public final String key;
    public final MonotonicId generation;

    public PrepareRequest(String key, MonotonicId generation) {
        super(RequestId.Prepare);
        this.key = key;
        this.generation = generation;
    }
}
