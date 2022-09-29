package distrib.patterns.twophasecommit;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;
import distrib.patterns.wal.Command;

public class CommitCommandRequest extends Request {
    byte[] command;
    public CommitCommandRequest(Command command) {
        this();
        this.command = command.serialize();
    }

    public byte[] getCommand() {
        return command;
    }

    private CommitCommandRequest() {
        super(RequestId.CommitRequest);
    }
}
