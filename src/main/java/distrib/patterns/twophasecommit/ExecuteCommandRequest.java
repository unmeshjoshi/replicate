package distrib.patterns.twophasecommit;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class ExecuteCommandRequest extends Request {
    public final byte[] command;

    public ExecuteCommandRequest(byte[] command) {
        super(RequestId.ExcuteCommandRequest);
        this.command = command;
    }
}
