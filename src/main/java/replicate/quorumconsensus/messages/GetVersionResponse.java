package replicate.quorumconsensus.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

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
