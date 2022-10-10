package replicator.twophasecommit.messages;

import replicator.common.Request;
import replicator.common.RequestId;

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
