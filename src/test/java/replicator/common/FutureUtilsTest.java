package replicator.common;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FutureUtilsTest {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    @Test
    public void shouldRetryAsyncExecution() throws ExecutionException, InterruptedException {
        final AtomicInteger attempt = new AtomicInteger(1);
        int maxAttempts = 5;
        CompletableFuture<Integer> future = FutureUtils.retryWithRandomDelay(() -> {
            if (attempt.incrementAndGet() == maxAttempts) {
                return CompletableFuture.completedFuture(4);
            }
            return CompletableFuture.failedFuture(new RuntimeException("Exception while execution"));
        }, maxAttempts, executor);

        assertEquals(4, future.get().intValue()); //block and wait for future.
        assertEquals(5, attempt.get()); //future should have completed after maxAttempts
    }

    @Test(expected = java.util.concurrent.ExecutionException.class)
    public void shouldFailAfterMaxAttempts() throws ExecutionException, InterruptedException {
        int maxAttempts = 5;
        CompletableFuture<Integer> future = FutureUtils.retryWithRandomDelay(() -> {
            return CompletableFuture.failedFuture(new RuntimeException("Exception while execution"));
        }, maxAttempts, executor);

        future.get();
    }

}