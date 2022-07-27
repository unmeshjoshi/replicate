package distrib.patterns.net.requestwaitinglist;

import distrib.patterns.common.SystemClock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RequestWaitingList<Key, Response> {
    private Map<Key, CallbackDetails> pendingRequests = new ConcurrentHashMap<>();
    public void add(Key key, RequestCallback<Response> callback) {
        pendingRequests.put(key, new CallbackDetails(callback, clock.nanoTime()));
    }

    private SystemClock clock;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private long expirationIntervalMillis = Long.MAX_VALUE; //do not expire for now.
    public RequestWaitingList(SystemClock clock) {
        this.clock = clock;
        executor.scheduleWithFixedDelay(this::expire, expirationIntervalMillis, expirationIntervalMillis, MILLISECONDS);
    }

    private void expire() {
        long now = clock.nanoTime();
        List<Key> expiredRequestKeys = getExpiredRequestKeys(now);
        expiredRequestKeys.stream().forEach(expiredRequestKey -> {
            CallbackDetails request = pendingRequests.remove(expiredRequestKey);
            request.requestCallback.onError(new TimeoutException("Request expired"));
        });
    }

    private List<Key> getExpiredRequestKeys(long now) {
        return pendingRequests.entrySet().stream().filter(entry -> entry.getValue().elapsedTime(now) > expirationIntervalMillis).map(e -> e.getKey()).collect(Collectors.toList());
    }

    public void handleResponse(Key key, Response response) {
        if (!pendingRequests.containsKey(key)) {
            return;
        }
        CallbackDetails callbackDetails = pendingRequests.remove(key);
        callbackDetails.getRequestCallback().onResponse(response);

    }

    public void handleError(int requestId, Throwable e) {
        CallbackDetails callbackDetails = pendingRequests.remove(requestId);
        callbackDetails.getRequestCallback().onError(e);
    }

}
