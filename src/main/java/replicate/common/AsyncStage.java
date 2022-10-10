package replicate.common;

import java.util.concurrent.CompletableFuture;

class AsyncStage<Req> {
    private Message<RequestOrResponse> message;
    private CompletableFuture<Req> request;

    public AsyncStage(Message<RequestOrResponse> message, CompletableFuture<Req> request) {
        this.message = message;
        this.request = request;
    }

    public CompletableFuture<Req> getRequest() {
        return request;
    }

    public Message<RequestOrResponse> getMessage() {
        return message;
    }
}
