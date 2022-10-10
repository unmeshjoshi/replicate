package replicator.twophasecommit.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class ProposeRequest extends Request {
    byte[] command;
    public ProposeRequest(byte[] serialize) {
        this();
        this.command = serialize;
    }

    public byte[] getCommand() {
        return command;
    }

    private ProposeRequest() {
        super(RequestId.ProposeRequest);
    }
}
