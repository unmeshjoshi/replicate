package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class PrepareRequest extends Request {
    public final Integer index;
    public final MonotonicId monotonicId;

    public PrepareRequest(Integer index, MonotonicId monotonicId) {
        super(RequestId.Prepare);
        this.index = index;
        this.monotonicId = monotonicId;
    }
}
