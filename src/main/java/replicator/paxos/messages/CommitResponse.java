package replicator.paxos.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class CommitResponse extends Request {
    public final boolean success;

    public CommitResponse(boolean success) {
        super(RequestId.CommitResponse);
        this.success = success;
    }
}
