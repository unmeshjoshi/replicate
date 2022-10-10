package replicate.twophaseexecution.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class ProposeResponse extends Request {
    private boolean isAccepted;

    public ProposeResponse(boolean isAccepted) {
        this();
        this.isAccepted = isAccepted;
    }

    public boolean isAccepted() {
        return isAccepted;
    }

    public ProposeResponse() {
        super(RequestId.ProposeResponse);
    }
}
