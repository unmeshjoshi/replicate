package replicate.chain;

import replicate.net.InetAddressAndPort;
import replicate.net.requestwaitinglist.RequestCallback;

import java.util.concurrent.CompletableFuture;

public class CompletionCallback<T> implements RequestCallback<T> {
    private final CompletableFuture<T> future = new CompletableFuture<>();

    @Override
    public void onResponse(T r, InetAddressAndPort fromNode) {
        future.complete(r);
    }

    @Override
    public void onError(Exception e) {
        future.completeExceptionally(e);
    }

    public CompletableFuture<T> getFuture() {
        return future;
    }
} 