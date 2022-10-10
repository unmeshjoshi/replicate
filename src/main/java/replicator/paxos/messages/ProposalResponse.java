package replicator.paxos.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class ProposalResponse extends Request {
    public final boolean success;

    public ProposalResponse(boolean success) {
        super(RequestId.ProposeResponse);
        this.success = success;
    }
}
