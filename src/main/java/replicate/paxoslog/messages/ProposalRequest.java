package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class ProposalRequest extends Request {
    public final MonotonicId generation;
    public final int index;
    public final byte[] proposedValue;

    public ProposalRequest(MonotonicId generation, int index, byte[] proposedValue) {
        super(RequestId.ProposeRequest);
        this.generation = generation;
        this.index = index;
        this.proposedValue = proposedValue;
    }
}
