package net.pipeline.nio;

import common.RequestOrResponse;
import net.InetAddressAndPort;
import singularupdatequeue.SingularUpdateQueue;

import java.io.IOException;
import java.util.function.Consumer;


public class NIOPipelinedConnection {
    private final SendThread sendThread;
    private final SingularUpdateQueue<SocketRequestOrResponse, Boolean> responseQueue;
    private final Consumer<SocketRequestOrResponse> consumer;
    public NIOPipelinedConnection(Consumer<SocketRequestOrResponse> consumer) throws IOException {
        this.consumer = consumer;
        sendThread = new SendThread(this::consumeRequest);
        responseQueue
                = new SingularUpdateQueue<>(request -> {
            consumer.accept(request);
            return true;
        });
    }

    public void queueRequest(InetAddressAndPort address, RequestOrResponse request) {
        try {
            sendThread.queueRequest(address, request);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        sendThread.start();
        responseQueue.start();
    }

    public void stop() {
        sendThread.shutdown();
    }

    private void consumeRequest(SocketResponse socketResponse) {
        RequestOrResponse respose = socketResponse.getResponse();
        responseQueue.submit(new SocketRequestOrResponse(socketResponse.getAddress(), respose));
    }
}


