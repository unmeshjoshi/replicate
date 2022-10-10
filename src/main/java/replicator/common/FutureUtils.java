package replicator.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.function.Supplier;


public class FutureUtils {
    private static Logger logger = LogManager.getLogger(FutureUtils.class);
    //Adapted from From https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/util/concurrent/FutureUtils.java
    /**
     * Retry the given operation with the given delay in between failures.
     *
     * @param operation to retry
     * @param attempt the attempt number
     * @param maxAttempts max allowed attempts.
     * @param scheduledExecutor executor to be used for the retry operation
     * @param <T> type of the result
     * @return Future which retries the given operation a given amount of times and delays the retry
     *     in case of failures
     */
    public static <T> CompletableFuture<T> retryWithRandomDelay(
            final Supplier<CompletableFuture<T>> operation,
            int maxAttempts,
            final ScheduledExecutorService scheduledExecutor) {

        final CompletableFuture<T> resultFuture = new CompletableFuture<>();

        retryOperationWithRandomDelay(
                resultFuture, operation, 1, maxAttempts, scheduledExecutor);

        return resultFuture;
    }

    private static <T> void retryOperationWithRandomDelay(
            final CompletableFuture<T> resultFuture,
            final Supplier<CompletableFuture<T>> operation,
            int attempt,
            int maxAttempts,
            final ScheduledExecutorService scheduledExecutor) {
        logger.info("Attempt " + attempt + " of maxAttemps " + maxAttempts);
        if (!resultFuture.isDone()) {
            final CompletableFuture<T> operationResultFuture = operation.get();
            operationResultFuture.whenComplete(
                    (t, throwable) -> {
                        if (throwable != null) {
                            throwable.printStackTrace();
                            logger.info("Attempt failed with " + throwable);

                            if (throwable instanceof CancellationException) {
                                resultFuture.completeExceptionally(
                                        new RetryException(
                                                "Operation future was cancelled.", throwable));
                            } else {
                             if (maxAttempts > attempt) {
                                    long retryDelayMillis = ThreadLocalRandom.current().nextInt(100);
                                    final ScheduledFuture<?> scheduledFuture =
                                            scheduledExecutor.schedule(
                                                    (Runnable)
                                                            () ->
                                                                    retryOperationWithRandomDelay(
                                                                            resultFuture,
                                                                            operation,
                                                                            attempt + 1,
                                                                            maxAttempts,
                                                                            scheduledExecutor),
                                                    retryDelayMillis,
                                                    TimeUnit.MILLISECONDS);

                                    resultFuture.whenComplete(
                                            (innerT, innerThrowable) ->
                                                    scheduledFuture.cancel(false));
                                } else {
                                    RetryException retryException =
                                            new RetryException(
                                                    "Could not complete the operation. " + maxAttempts + " Number of retries has been exhausted.",
                                                    throwable);
                                    resultFuture.completeExceptionally(retryException);
                                }
                            }
                        } else {
                            resultFuture.complete(t);
                        }
                    });

            resultFuture.whenComplete((t, throwable) -> operationResultFuture.cancel(false));
        }
    }

    public static class RetryException extends Exception {

        private static final long serialVersionUID = 3613470781274141862L;

        public RetryException(String message, Throwable cause) {
            super(message, cause);
        }

    }
}

