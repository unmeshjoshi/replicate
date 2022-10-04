package distrib.patterns.twophasecommit;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

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
        super(RequestId.CommitRequest);
    }
}
