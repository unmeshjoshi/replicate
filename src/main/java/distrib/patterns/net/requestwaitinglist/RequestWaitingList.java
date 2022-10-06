package distrib.patterns.net.requestwaitinglist;

import distrib.patterns.common.SystemClock;
import distrib.patterns.net.InetAddressAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RequestWaitingList<Key, Response> {
    private static Logger logger = LogManager.getLogger(RequestWaitingList.class);

    private Map<Key, CallbackDetails> pendingRequests = new ConcurrentHashMap<>();
    private InetAddressAndPort NONE;

    public void add(Key key, RequestCallback<Response> callback) {
        pendingRequests.put(key, new CallbackDetails(callback, clock.nanoTime()));
    }

    private SystemClock clock;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private Duration expirationDuration; //do not expire for now.
    public RequestWaitingList(SystemClock clock) {
        this(clock, Duration.ofMillis(2000));
    }
    public RequestWaitingList(SystemClock clock, Duration duration) {
        this.expirationDuration = duration;
        this.clock = clock;
        executor.scheduleWithFixedDelay(this::expire, expirationDuration.toMillis(), expirationDuration.toMillis(), MILLISECONDS);
    }

    private void expire() {
        long now = clock.nanoTime();
        List<Key> expiredRequestKeys = getExpiredRequestKeys(now);
        if (expiredRequestKeys.isEmpty()) {
            return;
        }
        logger.info("Expiring " + expiredRequestKeys);
        expiredRequestKeys.stream().forEach(expiredRequestKey -> {
            CallbackDetails request = pendingRequests.remove(expiredRequestKey);
            request.requestCallback.onError(new TimeoutException("Request expired"));
        });
    }

    private List<Key> getExpiredRequestKeys(long now) {
        return pendingRequests.entrySet().stream().filter(entry -> entry.getValue().elapsedTime(now) > expirationDuration.getNano()).map(e -> e.getKey()).collect(Collectors.toList());
    }

    public void handleResponse(Key key, Response response) {
        if (!pendingRequests.containsKey(key)) {
            return;
        }
        CallbackDetails callbackDetails = pendingRequests.remove(key);
        NONE = null;
        callbackDetails.getRequestCallback().onResponse(response, NONE);//TODO:Possibly use Optional

    }

    public void handleResponse(Key key, Response response, InetAddressAndPort fromNode) {
        if (!pendingRequests.containsKey(key)) {
            return;
        }
        CallbackDetails callbackDetails = pendingRequests.remove(key);
        callbackDetails.getRequestCallback().onResponse(response, fromNode);

    }

    public void handleError(int requestId, Exception e) {
        CallbackDetails callbackDetails = pendingRequests.remove(requestId);
        callbackDetails.getRequestCallback().onError(e);
    }

}
