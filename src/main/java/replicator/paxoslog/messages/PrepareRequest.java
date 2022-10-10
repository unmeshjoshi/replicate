package replicator.paxoslog.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;

public class PrepareRequest extends Request {
    public final Integer index;
    public final MonotonicId monotonicId;

    public PrepareRequest(Integer index, MonotonicId monotonicId) {
        super(RequestId.Prepare);
        this.index = index;
        this.monotonicId = monotonicId;
    }
}
