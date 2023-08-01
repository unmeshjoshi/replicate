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

/**
 * The nodes expecting messages from other nodes as response
 * to some messages, wait in a RequestWaitingList instance.
 * The messages are linked by a unique correlationId.
 * #see Replica:newCorrelationId
 * A callback is registered with each correlationId,
 * which is invoked once the message is received for that correlationId.
 *
 * The client can then use the future associated with the callback
 * to compose asynchronous actions.
 * @See AsyncQuorumCallback
 *
 *       ┌──────┐  ┌──────────────────┐  ┌─────────┐
 *       │athens│  │RequestWaitingList│  │byzantium│
 *       └──┬───┘  └────────┬─────────┘  └────┬────┘
 * executeRequest           │                 │
 *   ─ ─ ─ ─>               │                 │
 *          │               │                 │
 *          │               │   new           │       ┌────────┐
 *          │ ───────────────────────────────────────>│Callback│
 *          │               │                 │       └───┬────┘
 *          │               │                 │           │  new  ┌──────┐
 *          │               │                 │           │ ─────>│Future│
 *          │               │                 │           │       └──┬───┘
 *          │            message 1            │           │          │
 *          │ ───────────────────────────────>│           │          │
 *          │               │                 │           │          │
 *          │add(1, Callback)                 │           │          │
 *          │ ──────────────>                 │           │          │
 *          │               │                 │           │          │
 *          │           response 1            │           │          │
 *          │ <─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│           │          │
 *          │               │                 │           │          │
 *         onResponse(response)               │           │          │
 *          │ ──────────────>                 │           │          │
 *          │               │                 │           │          │
 *          │               │          onResponse         │          │
 *          │               │ ────────────────────────────>          │
 *          │               │                 │           │          │
 *          │               │                 │           │ complete │
 *          │               │                 │           │  ─ ─ ─ ─ >
 *          │               │                 │           │          │
 *          │               │                 │           │          │
 *          │ <─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *          │               │                 │           │          │
 *  response│               │                 │           │          │
 *  <─ ─ ─ ─                │                 │           │          │
 *          │               │                 │           │          │
 *          │               │                 │           │          │
 */

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
        this(clock, Duration.ofMillis(1000)); //TODO: Keeping this as 1
        // second occasionally expires some get requests and fails read-repair tests
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
            CallbackDetails cb = pendingRequests.remove(expiredRequestKey);
            cb.requestCallback.onError(new TimeoutException("Request expired"));
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
