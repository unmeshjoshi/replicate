package distrib.patterns.net.requestwaitinglist;

class CallbackDetails {
    RequestCallback requestCallback;
    long createTime;

    public CallbackDetails(RequestCallback requestCallback, long createTime) {
        this.requestCallback = requestCallback;
        this.createTime = createTime;
    }

    public RequestCallback getRequestCallback() {
        return requestCallback;
    }

    public long elapsedTime(long now) {
        return now - createTime;
    }
}
