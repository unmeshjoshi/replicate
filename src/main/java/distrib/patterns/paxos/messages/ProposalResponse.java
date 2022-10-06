package distrib.patterns.paxos.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class ProposalResponse extends Request {
    public final boolean success;

    public ProposalResponse(boolean success) {
        super(RequestId.ProposeResponse);
        this.success = success;
    }
}
