package distrib.patterns.paxos.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class CommitResponse extends Request {
    boolean success;

    public CommitResponse(boolean success) {
        this();
        this.success = success;
    }

    //for jackson
    private CommitResponse() {
        super(RequestId.CommitResponse);
    }
}
