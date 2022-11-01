package replicate.twophaseexecution.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class ProposeResponse extends MessagePayload {
    private boolean isAccepted;

    public ProposeResponse(boolean isAccepted) {
        this();
        this.isAccepted = isAccepted;
    }

    public boolean isAccepted() {
        return isAccepted;
    }

    public ProposeResponse() {
        super(MessageId.ProposeResponse);
    }
}
