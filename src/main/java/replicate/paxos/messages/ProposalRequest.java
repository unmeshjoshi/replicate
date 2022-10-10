package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class ProposalRequest extends Request {
    private MonotonicId monotonicId;
    private String proposedValue;

    public ProposalRequest(MonotonicId monotonicId, String proposedValue) {
        this();
        this.monotonicId = monotonicId;
        this.proposedValue = proposedValue;
    }

    public MonotonicId getMonotonicId() {
        return monotonicId;
    }

    public String getProposedValue() {
        return proposedValue;
    }
    //for jackson
    private ProposalRequest() {
        super(RequestId.ProposeRequest);
    }
}
