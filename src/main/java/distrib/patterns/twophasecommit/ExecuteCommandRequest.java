package distrib.patterns.twophasecommit;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;
import distrib.patterns.wal.Command;

public class ExecuteCommandRequest extends Request {
    private byte[] command;

    public ExecuteCommandRequest(Command command) {
        this();
        this.command = command.serialize();
    }

    public byte[] getCommand() {
        return command;
    }

    private ExecuteCommandRequest() {
        super(RequestId.ExcuteCommandRequest);
    }
}
