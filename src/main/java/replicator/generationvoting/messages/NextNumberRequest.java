package replicator.generationvoting.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class NextNumberRequest extends Request {
    public NextNumberRequest() {
        super(RequestId.NextNumberRequest);
    }
}
