package replicate.twophaseexecution.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class ExecuteCommandRequest extends MessagePayload {
    public final byte[] command;

    public ExecuteCommandRequest(byte[] command) {
        super(MessageId.ExcuteCommandRequest);
        this.command = command;
    }
}
