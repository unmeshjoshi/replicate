package replicate.paxos.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class CommitResponse extends MessagePayload {
    public final boolean success;

    public CommitResponse(boolean success) {
        super(MessageId.CommitResponse);
        this.success = success;
    }
}
