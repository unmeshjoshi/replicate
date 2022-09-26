package distrib.patterns.quorumconsensus.messages;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

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
