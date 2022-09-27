package distrib.patterns.twophasecommit;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;
import distrib.patterns.wal.Command;

public class ProposeRequest extends Request {
    byte[] command;
    public ProposeRequest(Command command) {
        this();
        this.command = command.serialize();
    }

    public byte[] getCommand() {
        return command;
    }

    private ProposeRequest() {
        super(RequestId.ProposeRequest);
    }
}
