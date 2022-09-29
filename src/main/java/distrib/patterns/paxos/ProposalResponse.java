package distrib.patterns.paxos;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class ProposalResponse extends Request {
    boolean success;

    public ProposalResponse(boolean success) {
        this();
        this.success = success;
    }

    private ProposalResponse() {
        super(RequestId.ProposeResponse);
    }
}
