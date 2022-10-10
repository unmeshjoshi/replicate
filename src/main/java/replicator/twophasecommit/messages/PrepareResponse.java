package replicator.twophasecommit.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class PrepareResponse extends Request {
    public final byte[] command;
    public PrepareResponse(byte[] command) {
        super(RequestId.Promise);
        this.command = command;
    }
}
