package net.pipeline.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import common.JsonSerDes;
import common.RequestOrResponse;
import net.InetAddressAndPort;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NioClient {
    private static Logger logger = LogManager.getLogger(NioClient.class.getName());

    private int correlationId;
    List<RequestOrResponse> responses = new CopyOnWriteArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    RequestLimitingPipelinedConnection client = new RequestLimitingPipelinedConnection(response -> {
        responses.add(response);
    });
    private long readTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(2000);

    public NioClient() throws IOException {
    }

    public void start() {
        client.start();
    }

    public void submitRequest(InetAddressAndPort address1, String message) {
        executorService.submit(() -> {
            try {
                client.send(address1, newRequest(1, message));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public RequestOrResponse submitRequestBlocking(InetAddressAndPort address1, String message) throws ReadTimeoutException {
        long requestTime = System.nanoTime();
        RequestOrResponse request = newRequest(1, message);
        executorService.submit(() -> {
            try {
                client.send(address1, request);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        while (true) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Responses=" + responses);
            for (RequestOrResponse response : responses) {
                logger.info("Checking response for " + response.getCorrelationId() + " and " + request.getCorrelationId());
                if (response.getCorrelationId() == request.getCorrelationId()) {
                    return response;
                }
            }

            long now = System.nanoTime();
            if (now - requestTime > readTimeoutNanos)
                throw new ReadTimeoutException("Did not receive response in " + readTimeoutNanos + " ms.");
        }
    }


    private RequestOrResponse newRequest(Integer serverId, String message) {
        return new RequestOrResponse(serverId, JsonSerDes.serialize(message), correlationId++);
    }

    public List<RequestOrResponse> getResponses() {
        return responses;
    }

}
