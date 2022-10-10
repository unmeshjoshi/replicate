package replicator.paxos.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;

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
