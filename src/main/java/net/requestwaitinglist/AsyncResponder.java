package net.requestwaitinglist;

import common.Response;

import java.util.concurrent.CompletableFuture;

public class AsyncResponder {
    int totalResponses;
    int receivedResponse;
    CompletableFuture future = new CompletableFuture();

    public void ackResponse(Response r) {
        receivedResponse++;

    }
}
