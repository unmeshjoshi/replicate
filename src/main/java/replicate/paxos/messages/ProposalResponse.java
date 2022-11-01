package replicate.paxos.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class ProposalResponse extends MessagePayload {
    public final boolean success;

    public ProposalResponse(boolean success) {
        super(MessageId.ProposeResponse);
        this.success = success;
    }
}
