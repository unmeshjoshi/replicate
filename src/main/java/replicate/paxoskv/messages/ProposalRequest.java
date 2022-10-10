package replicate.paxoskv.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class ProposalRequest extends Request {
    public final MonotonicId generation;
    public final String key;
    public final String proposedValue;

    public ProposalRequest(MonotonicId generation, String key, String proposedValue) {
        super(RequestId.ProposeRequest);
        this.generation = generation;
        this.key = key;
        this.proposedValue = proposedValue;
    }
}
