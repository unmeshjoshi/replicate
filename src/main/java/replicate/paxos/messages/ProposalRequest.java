package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class ProposalRequest extends MessagePayload {
    public final MonotonicId monotonicId;
    public final byte[] proposedValue;

    public ProposalRequest(MonotonicId monotonicId, byte[] proposedValue) {
        super(MessageId.ProposeRequest);
        this.monotonicId = monotonicId;
        this.proposedValue = proposedValue;
    }
}
