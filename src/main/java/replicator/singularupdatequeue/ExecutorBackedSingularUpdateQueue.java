package replicator.singularupdatequeue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

public class ExecutorBackedSingularUpdateQueue<P, R> {
    private Function<P, R> updateHandler;
    private SingularUpdateQueue<R, ?> next;
    private ExecutorService singleThreadedExecutor = Executors.newFixedThreadPool(1);

    public ExecutorBackedSingularUpdateQueue(Function<P, R> updateHandler) {
        this.updateHandler = updateHandler;
    }

    public ExecutorBackedSingularUpdateQueue(Function<P, R> updateHandler, SingularUpdateQueue<R, ?> next) {
        this.updateHandler = updateHandler;
        this.next = next;
    }

    public void start() {
    }

    public void submit(P request) {
        var completableFuture = new CompletableFuture<R>();
        Future<?> submit = singleThreadedExecutor.submit(() -> {
            R update = updateHandler.apply(request);
            completableFuture.complete(update);
        });

        if (next != null) {
           completableFuture.thenAccept(s -> next.submit(s));
        }
    }
}
