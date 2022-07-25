package net.requestbatch;

import common.*;
import net.BatchResponse;
import net.InetAddressAndPort;
import net.SocketClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import requests.SetValueRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Client {
    static Logger logger = LogManager.getLogger(Client.class);
    Sender sender;
    private volatile long lastEnqueueTimestamp;
    private volatile int requestNumber;
    private SystemClock clock;

    //<codeFragment name="client">
    public Client(Config config, InetAddressAndPort serverAddress, SystemClock clock) {
        this.clock = clock;
        this.sender = new Sender(config, serverAddress, clock);
        this.sender.start();
    }
    //</codeFragment>

    //<codeFragment name="send">
    LinkedBlockingQueue<RequestEntry> requests = new LinkedBlockingQueue<>();

    public CompletableFuture send(SetValueRequest setValueRequest) {
        int requestId = enqueueRequest(setValueRequest);
        CompletableFuture responseFuture = trackPendingRequest(requestId);
        return responseFuture;
    }

    private int enqueueRequest(SetValueRequest setValueRequest) {
        int requestId = nextRequestId();
        byte[] requestBytes = serialize(setValueRequest, requestId);
        requests.add(new RequestEntry(requestBytes, clock.nanoTime()));
        return requestId;
    }
    private int nextRequestId() {
        return requestNumber++;
    }
    //</codeFragment>

    private byte[] serialize(SetValueRequest setValueRequest, int requestId) {
        byte[] serializedRequest = JsonSerDes.serialize(setValueRequest);
        RequestOrResponse request = new RequestOrResponse(RequestId.SetValueRequest.getId(), serializedRequest, requestId);
        byte[] serialize = JsonSerDes.serialize(request);
        return serialize;
    }
    //<codeFragment name="trackPendingRequest">
    Map<Integer, CompletableFuture> pendingRequests = new ConcurrentHashMap<>();

    private CompletableFuture trackPendingRequest(Integer correlationId) {
        CompletableFuture responseFuture = new CompletableFuture();
        pendingRequests.put(correlationId, responseFuture);
        return responseFuture;
    }
    //</codeFragment>

    public void start() {
        sender.start();
    }

    public void shutdown() {
        sender.shutdown();
    }


    class Sender extends Thread {
        int MAX_BATCH_SIZE_BYTES = 16000;
        private final Config config;
        boolean isRunning = true;
        private InetAddressAndPort address;
        private SystemClock clock;
        private int requestId;

        public Sender(Config config, InetAddressAndPort address, SystemClock clock) {
            this.config = config;
            this.address = address;
            this.clock = clock;
        }


        //<codeFragment name="sender">
        @Override
        public void run() {
            while (isRunning) {
                boolean maxWaitTimeElapsed = requestsWaitedFor(config.getMaxBatchWaitTime());
                boolean maxBatchSizeReached = maxBatchSizeReached(requests);
                if (maxWaitTimeElapsed || maxBatchSizeReached) {
                    RequestBatch batch = createBatch(requests);
                    try {
                        BatchResponse batchResponse = sendBatchRequest(batch, address);
                        handleResponse(batchResponse);

                    } catch (IOException e) {
                        batch.getPackedRequests().stream().forEach(r -> {
                            pendingRequests.get(r.getCorrelationId()).completeExceptionally(e);
                        });
                    }
                }
            }
        }

        private RequestBatch createBatch(LinkedBlockingQueue<RequestEntry> requests) {
            RequestBatch batch = new RequestBatch(MAX_BATCH_SIZE_BYTES);
            RequestEntry entry = requests.peek();
            while (entry != null && batch.hasSpaceFor(entry.getRequest())) {
                batch.add(entry.getRequest());
                requests.remove(entry);
                entry = requests.peek();
            }
            return batch;
        }
        //</codeFragment>

        private BatchResponse sendBatchRequest(RequestBatch batch, InetAddressAndPort address) throws IOException {
            SocketClient client = new SocketClient(address);
            try {
                RequestOrResponse response = client.blockingSend(new RequestOrResponse(RequestId.BatchRequest.getId(), JsonSerDes.serialize(batch), requestId++));
                BatchResponse batchResponse = JsonSerDes.deserialize(response.getMessageBodyJson(), BatchResponse.class);
                return batchResponse;
            } finally {
                client.close();
            }
        }


        //<codeFragment name="requestsWaitedFor">
        private boolean requestsWaitedFor(long batchingWindowInMs) {
            RequestEntry oldestPendingRequest = requests.peek();
            if (oldestPendingRequest == null) {
                return false;
            }
            long oldestEntryWaitTime = clock.nanoTime() - oldestPendingRequest.createdTime;
            return oldestEntryWaitTime > batchingWindowInMs;
        }
        //</codeFragment>

        //<codeFragment name="maxBatchSizeReached">
        private boolean maxBatchSizeReached(Queue<RequestEntry> requests) {
            return accumulatedRequestSize(requests) > MAX_BATCH_SIZE_BYTES;
        }

        private int accumulatedRequestSize(Queue<RequestEntry> requests) {
            return requests.stream().map(re -> re.size()).reduce((r1, r2) -> r1 + r2).orElse(0);
        }
        //</codeFragment>

        public void shutdown() {
            isRunning = false;
        }
    }

    //<codeFragment name="handleResponse">
    private void handleResponse(BatchResponse batchResponse) {
        List<RequestOrResponse> responseList = batchResponse.getResponseList();
        logger.debug("Completing requests from " + responseList.get(0).getCorrelationId() + " to " + responseList.get(responseList.size() - 1).getCorrelationId());
        responseList.stream().forEach(r -> {
            CompletableFuture completableFuture = pendingRequests.remove(r.getCorrelationId());
            if (completableFuture != null) {
                completableFuture.complete(r);
            } else {
                logger.error("no pending request for " + r.getCorrelationId());
            }
        });
    }
    //</codeFragment>
}
