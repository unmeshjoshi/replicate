package replicator.common;

public class Request {
    public final RequestId requestId;

    public Request(RequestId requestId) {
        this.requestId = requestId;
    }

    public RequestId getRequestId() {
        return requestId;
    }
}
