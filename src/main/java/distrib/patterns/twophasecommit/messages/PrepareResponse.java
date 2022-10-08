package distrib.patterns.twophasecommit.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class PrepareResponse extends Request {
    public final byte[] command;
    public PrepareResponse(byte[] command) {
        super(RequestId.Promise);
        this.command = command;
    }
}
