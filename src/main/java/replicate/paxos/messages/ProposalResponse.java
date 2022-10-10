package replicate.paxos.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class ProposalResponse extends Request {
    public final boolean success;

    public ProposalResponse(boolean success) {
        super(RequestId.ProposeResponse);
        this.success = success;
    }
}
