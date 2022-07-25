package net.pipeline.nio;

import common.RequestOrResponse;
import net.InetAddressAndPort;

import java.io.IOException;
import java.util.function.Consumer;

class SendThread extends Thread {
    NioSelector selector;
    private volatile boolean isRunning;

    public SendThread(Consumer<SocketResponse> consumer) throws IOException {
        this.selector = new NioSelector(consumer);
    }

    public void queueRequest(InetAddressAndPort address, RequestOrResponse request) {
        selector.initConnectionAndQueueRequest(address, request);
    }

    @Override
    public void run() {
        isRunning = true;
        while (isRunning) {
            try {
                selector.poll();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        this.isRunning = false;
        selector.stop();
    }
}
