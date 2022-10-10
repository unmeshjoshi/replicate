package replicator.quorumconsensus.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;

public class GetVersionResponse extends Request {
    MonotonicId id;

    public GetVersionResponse(MonotonicId id) {
        super(RequestId.GetVersionResponse);
        this.id = id;
    }

    //for jackson
    private GetVersionResponse() {
        super(RequestId.GetVersionResponse);
    }

    public MonotonicId getVersion() {
        return id;
    }
}
