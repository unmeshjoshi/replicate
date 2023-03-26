package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

import java.time.Duration;

public class ProposalRequest extends MessagePayload {
    public final MonotonicId generation;
    public final int index;
    public final byte[] proposedValue;
    public ProposalRequest(MonotonicId generation, int index, byte[] proposedValue) {
        super(MessageId.ProposeRequest);
        this.generation = generation;
        this.index = index;
        this.proposedValue = proposedValue;
    }
}
