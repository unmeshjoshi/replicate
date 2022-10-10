package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class PrepareRequest extends Request {
    public MonotonicId monotonicId;

    public PrepareRequest(MonotonicId monotonicId) {
        this();
        this.monotonicId = monotonicId;
    }


    //for jackson
    private PrepareRequest() {
        super(RequestId.Prepare);
    }
}
