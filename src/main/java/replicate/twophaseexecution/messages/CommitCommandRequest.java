package replicate.twophaseexecution.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class CommitCommandRequest extends Request {
    byte[] command;
    public CommitCommandRequest(byte[] serialize) {
        this();
        this.command = serialize;
    }

    public byte[] getCommand() {
        return command;
    }

    private CommitCommandRequest() {
        super(RequestId.Commit);
    }
}
