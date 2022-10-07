package distrib.patterns.net.requestwaitinglist;

import java.time.Duration;

class CallbackDetails {
    RequestCallback requestCallback;
    long createTimeNanos;

    public CallbackDetails(RequestCallback requestCallback, long createTimeNanos) {
        this.requestCallback = requestCallback;
        this.createTimeNanos = createTimeNanos;
    }

    public RequestCallback getRequestCallback() {
        return requestCallback;
    }

    public long elapsedTimeNanos(long nowNanos) {
        return nowNanos - createTimeNanos;
    }

    boolean isExpired(Duration timeout, long nowNanos) {
        return elapsedTimeNanos(nowNanos) >= timeout.toNanos();
    }
}
