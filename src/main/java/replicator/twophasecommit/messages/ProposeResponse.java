package replicator.twophasecommit.messages;

import replicator.common.Request;
import replicator.common.RequestId;

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
