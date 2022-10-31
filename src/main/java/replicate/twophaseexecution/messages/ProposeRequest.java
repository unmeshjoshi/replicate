package replicate.twophaseexecution.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class ProposeRequest extends MessagePayload {
    byte[] command;
    public ProposeRequest(byte[] serialize) {
        this();
        this.command = serialize;
    }

    public byte[] getCommand() {
        return command;
    }

    private ProposeRequest() {
        super(MessageId.ProposeRequest);
    }
}
