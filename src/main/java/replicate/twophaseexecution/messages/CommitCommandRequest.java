package replicate.twophaseexecution.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class CommitCommandRequest extends MessagePayload {
    byte[] command;
    public CommitCommandRequest(byte[] serialize) {
        this();
        this.command = serialize;
    }

    public byte[] getCommand() {
        return command;
    }

    private CommitCommandRequest() {
        super(MessageId.Commit);
    }
}
