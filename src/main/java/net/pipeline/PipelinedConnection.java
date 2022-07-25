package net.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import common.Logging;
import common.RequestOrResponse;
import net.InetAddressAndPort;
import net.SingleSocketChannel;
import singularupdatequeue.SingularUpdateQueue;

import java.io.IOException;
import java.util.function.Consumer;

public class PipelinedConnection {
    private static Logger logger = LogManager.getLogger(PipelinedConnection.class.getName());

    private ResponseThread responseThread;
    private SingleSocketChannel singleSocketChannel;

    private final InetAddressAndPort address;
    private final long readTimeout;
    private final Consumer<RequestOrResponse> consumer;

    final SingularUpdateQueue<RequestOrResponse, Boolean> requestQueue;
    final SingularUpdateQueue<RequestOrResponse, Boolean> responseQueue;

    public PipelinedConnection(InetAddressAndPort address, long readTimeout, Consumer<RequestOrResponse> consumer) {
        this.address = address;
        this.readTimeout = readTimeout;
        this.consumer = consumer;
        responseQueue
                = new SingularUpdateQueue<>(request -> {
            consumer.accept(request);
            return true;
        });

        requestQueue = new SingularUpdateQueue<>((request) -> {
            try {
                return call(request);
            } catch (IOException e) {
                logger.error(e);
                throw new RuntimeException(e);
            }
        });

        startConnection(address, readTimeout);
    }

    private void startConnection(InetAddressAndPort address, long readTimeout) {
        try {
            logger.info("Starting new connectiont to " + address);

            singleSocketChannel = new SingleSocketChannel(address, Math.toIntExact(readTimeout));
            responseThread = new ResponseThread(singleSocketChannel);
            responseThread.start();

            logger.info("Connected successfully " + address);
        } catch (IOException e) {
            logger.info("Could not connect to " + address);
            logger.error(e);
        }
    }

    public InetAddressAndPort getAddress() {
        return address;
    }

    public void send(RequestOrResponse request) {
        requestQueue.submit(request);
    }

    private boolean call(RequestOrResponse request) throws IOException {
        if (singleSocketChannel == null || !singleSocketChannel.isConnected()) {
            startConnection(address, readTimeout);
        }

        if (singleSocketChannel != null) {
            singleSocketChannel.sendOneWay(request);
        }
        return true;
    }

    public void start() {
        requestQueue.start();
        responseQueue.start();
    }

    public void shutdown() {
        requestQueue.shutdown();
        responseQueue.shutdown();
        if (responseThread != null) {
            responseThread.shutdown();
        }
        if (singleSocketChannel != null) {
            singleSocketChannel.stop();
        }
    }

    //<codeFragment name="ResponseThread">
    class ResponseThread extends Thread implements Logging {
        private volatile boolean isRunning = false;
        private SingleSocketChannel socketChannel;

        public ResponseThread(SingleSocketChannel socketChannel) {
            this.socketChannel = socketChannel;
        }

        @Override
        public void run() {
            try {
                isRunning = true;
                logger.info("Starting responder thread = " + isRunning);
                while (isRunning) {
                    doWork();
                }

            } catch (IOException e) {
                e.printStackTrace();
                getLogger().error(e); //thread exits if stopped or there is IO error
            }
        }

        public void doWork() throws IOException {
            RequestOrResponse response = socketChannel.read();
            logger.info("Read Response = " + response);
            processResponse(response);
        }

        //</codeFragment>

        private void processResponse(RequestOrResponse response) {
            responseQueue.submit(response);
        }

        public void shutdown() {
            isRunning = false;
        }
    }
}
