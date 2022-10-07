package distrib.patterns.net.requestwaitinglist;

import distrib.patterns.common.SystemClock;
import distrib.patterns.vsr.CompletionCallback;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CallbackDetailsTest {

    @Test
    public void expiresAfterTimeout() {
        long createTimeNanos = 39433734125062l;
        CallbackDetails cb = new CallbackDetails(new CompletionCallback(), createTimeNanos);
        int timeoutMillis = 900;

        long elapsedTime = createTimeNanos + TimeUnit.NANOSECONDS.convert(Duration.ofMillis(timeoutMillis));
        SystemClock clock = new TestClock(elapsedTime);
        assertTrue(cb.isExpired(Duration.ofMillis(timeoutMillis), clock.nanoTime()));
    }

    @Test
    public void doesNotExpireBeforeTimeout() {
        long createTimeNanos = 39433734125062l;
        CallbackDetails cb = new CallbackDetails(new CompletionCallback(), createTimeNanos);
        int timeoutMillis = 900;

        long elapsedTime = createTimeNanos + TimeUnit.NANOSECONDS.convert(Duration.ofMillis(timeoutMillis - 10));
        SystemClock clock = new TestClock(elapsedTime);
        assertFalse(cb.isExpired(Duration.ofMillis(timeoutMillis), clock.nanoTime()));
    }

}