package replicate.twophaseexecution.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class PrepareRequest extends MessagePayload {
    public PrepareRequest() {
        super(MessageId.Prepare);
    }
}
