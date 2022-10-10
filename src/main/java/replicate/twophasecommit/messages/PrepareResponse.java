package replicate.twophasecommit.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class PrepareResponse extends Request {
    public final byte[] command;
    public PrepareResponse(byte[] command) {
        super(RequestId.Promise);
        this.command = command;
    }
}
