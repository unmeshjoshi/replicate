package distrib.patterns.paxos.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class CommitResponse extends Request {
    public final boolean success;

    public CommitResponse(boolean success) {
        super(RequestId.CommitResponse);
        this.success = success;
    }
}
