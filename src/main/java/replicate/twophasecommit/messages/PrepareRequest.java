package replicate.twophasecommit.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class PrepareRequest extends Request {
    public PrepareRequest() {
        super(RequestId.Prepare);
    }
}
