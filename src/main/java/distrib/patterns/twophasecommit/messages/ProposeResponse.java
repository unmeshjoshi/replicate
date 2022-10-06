package distrib.patterns.twophasecommit.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

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
