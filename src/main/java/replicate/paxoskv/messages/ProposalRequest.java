package replicate.paxoskv.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class ProposalRequest extends Request {
    public final MonotonicId generation;
    public final String key;
    public final byte[] proposedValue;

    public ProposalRequest(MonotonicId generation, String key, byte[] proposedValue) {
        super(RequestId.ProposeRequest);
        this.generation = generation;
        this.key = key;
        this.proposedValue = proposedValue;
    }
}
