package distrib.patterns.paxos.messages;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

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
