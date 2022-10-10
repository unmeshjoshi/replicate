package replicator.paxoskv.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;

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
