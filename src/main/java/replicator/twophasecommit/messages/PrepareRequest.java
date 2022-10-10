package replicator.twophasecommit.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class PrepareRequest extends Request {
    public PrepareRequest() {
        super(RequestId.Prepare);
    }
}
