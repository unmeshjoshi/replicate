package distrib.patterns.generation;

import distrib.patterns.common.RequestId;

class Request {
    RequestId requestId;

    public Request(RequestId requestId) {
        this.requestId = requestId;
    }

    public RequestId getRequestId() {
        return requestId;
    }
}

public class NextNumberRequest extends Request {
    public NextNumberRequest() {
        super(RequestId.NextNumberRequest);
    }
}
