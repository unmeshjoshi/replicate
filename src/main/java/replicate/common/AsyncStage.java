package replicate.common;

import java.util.concurrent.CompletableFuture;

class AsyncStage<T> {
    private Message<RequestOrResponse> message;
    private CompletableFuture<T> response;

    public AsyncStage(Message<RequestOrResponse> message, CompletableFuture<T> response) {
        this.message = message;
        this.response = response;
    }

    public CompletableFuture<T> getResponse() {
        return response;
    }

    public Message<RequestOrResponse> getMessage() {
        return message;
    }
}
