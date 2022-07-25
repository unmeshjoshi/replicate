package net.pipeline.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import common.RequestOrResponse;
import net.InetAddressAndPort;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class InFlightRequest {
    private RequestOrResponse request;
    private long timeoutMs;
    private long createdTime;

    public InFlightRequest(RequestOrResponse request, long timeoutMs) {
        this.request = request;
        this.timeoutMs = timeoutMs;
        this.createdTime = System.nanoTime();
    }

    public RequestOrResponse getRequest() {
        return request;
    }

    public boolean hasExpired() {
        long now = System.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(now - createdTime) > timeoutMs;
    }

    public long getCreatedTime() {
        return createdTime;
    }
}
public class RequestLimitingPipelinedConnection {
    private static Logger logger = LogManager.getLogger(RequestLimitingPipelinedConnection.class.getName());

    private final NIOPipelinedConnection pipelinedConnection;
    private Consumer<RequestOrResponse> responseConsumer;

    public RequestLimitingPipelinedConnection(Consumer<RequestOrResponse> responseConsumer) throws IOException {
        this.responseConsumer = responseConsumer;
        pipelinedConnection = new NIOPipelinedConnection(this::consume);
    }

    //<codeFragment name="inFlightRequestReceipt">
    private void consume(SocketRequestOrResponse response) {
        Integer correlationId = response.getRequest().getCorrelationId();
        Queue<RequestOrResponse> requestsForAddress = inflightRequests.get(response.getAddress());
        RequestOrResponse first = requestsForAddress.peek();
        if (correlationId != first.getCorrelationId()) {
            throw new RuntimeException("First response should be for the first request");
        }
        requestsForAddress.remove(first);
        responseConsumer.accept(response.getRequest());
    }
    //</codeFragment>

    //<codeFragment name="maxInflightRequests">
    private final Map<InetAddressAndPort, ArrayBlockingQueue<RequestOrResponse>> inflightRequests = new ConcurrentHashMap<>();
    private int maxInflightRequests = 5;
    public void send(InetAddressAndPort to, RequestOrResponse request) throws InterruptedException {
        ArrayBlockingQueue<RequestOrResponse> requestsForAddress = inflightRequests.get(to);
        if (requestsForAddress == null) {
            requestsForAddress = new ArrayBlockingQueue<>(maxInflightRequests);
            inflightRequests.put(to, requestsForAddress);
        }
        requestsForAddress.put(request);
    //</codeFragment>
       pipelinedConnection.queueRequest(to, request);
    }

    public void start() {
        pipelinedConnection.start();
    }
}
