package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class PrepareRequest extends Request {
    public final MonotonicId monotonicId;

    public PrepareRequest(MonotonicId monotonicId) {
        super(RequestId.Prepare);
        this.monotonicId = monotonicId;
    }
}
