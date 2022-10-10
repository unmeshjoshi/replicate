package replicate.paxos.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class CommitResponse extends Request {
    public final boolean success;

    public CommitResponse(boolean success) {
        super(RequestId.CommitResponse);
        this.success = success;
    }
}
