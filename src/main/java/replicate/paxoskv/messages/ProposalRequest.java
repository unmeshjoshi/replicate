package replicate.paxoskv.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class ProposalRequest extends MessagePayload {
    public final MonotonicId generation;
    public final String key;
    public final byte[] proposedValue;

    public ProposalRequest(MonotonicId generation, String key, byte[] proposedValue) {
        super(MessageId.ProposeRequest);
        this.generation = generation;
        this.key = key;
        this.proposedValue = proposedValue;
    }
}
