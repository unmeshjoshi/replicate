package replicate.net.requestwaitinglist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.SystemClock;
import replicate.net.InetAddressAndPort;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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
        long now = clock.nanoTime();
        logger.debug("RequestWaitingList adding " + key + " at " + now);
        pendingRequests.put(key, new CallbackDetails(callback, now));
    }

    private SystemClock clock;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private Duration expirationDuration; //do not expire for now.
    public RequestWaitingList(SystemClock clock) {
        this(clock, Duration.ofMillis(2000)); //TODO: Keeping this as 1 second occasionally expires some get requests and fails read-repair tests
    }
    public RequestWaitingList(SystemClock clock, Duration duration) {
        this.expirationDuration = duration;
        this.clock = clock;
        executor.scheduleWithFixedDelay(this::expire, expirationDuration.toMillis(), expirationDuration.toMillis(), MILLISECONDS);
    }

    private void expire() {
        List<Key> expiredRequestKeys = getExpiredRequestKeys();
        if (expiredRequestKeys.isEmpty()) {
            return;
        }
        logger.info("Expiring " + expiredRequestKeys);
        expiredRequestKeys.stream().forEach(expiredRequestKey -> {
            CallbackDetails request = pendingRequests.remove(expiredRequestKey);
            request.requestCallback.onError(new TimeoutException("Request expired"));
        });
    }

    private List<Key> getExpiredRequestKeys() {
        return pendingRequests.entrySet().stream().filter(entry -> entry.getValue().isExpired(this.expirationDuration, clock.nanoTime())).map(e -> e.getKey()).collect(Collectors.toList());
    }

    public void handleResponse(Key key, Response response) {
        if (!pendingRequests.containsKey(key)) {
            return;
        }
        logger.debug("RequestWaitingList received response for " + key + " at " + clock.nanoTime());
        CallbackDetails callbackDetails = pendingRequests.remove(key);
        NONE = null;
        callbackDetails.getRequestCallback().onResponse(response, NONE);//TODO:Possibly use Optional

    }

    public void handleResponse(Key key, Response response, InetAddressAndPort fromNode) {
        logger.debug("RequestWaitingList received response for " + key + " at " + clock.nanoTime());

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
