package replicator.twophasecommit.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class ExecuteCommandRequest extends Request {
    public final byte[] command;

    public ExecuteCommandRequest(byte[] command) {
        super(RequestId.ExcuteCommandRequest);
        this.command = command;
    }
}
