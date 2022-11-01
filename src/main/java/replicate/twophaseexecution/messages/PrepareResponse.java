package replicate.twophaseexecution.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class PrepareResponse extends MessagePayload {
    public final byte[] command;
    public PrepareResponse(byte[] command) {
        super(MessageId.Promise);
        this.command = command;
    }
}
