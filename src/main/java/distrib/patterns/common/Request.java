package distrib.patterns.common;

public class Request {
    RequestId requestId;

    public Request(RequestId requestId) {
        this.requestId = requestId;
    }

    public RequestId getRequestId() {
        return requestId;
    }
}
