package net.nioserver.kafkastyle;

import common.Logging;
import net.InetAddressAndPort;
import net.NetworkException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.List;
import java.util.concurrent.CountDownLatch;


class Acceptor extends Thread implements Logging {
    private final ServerSocketChannel serverChannel;
    InetAddressAndPort endPoint;
    Integer sendBufferSize;
    Integer recvBufferSize;
    private List<Processor> processors;
    private final Selector nioSelector;
    private volatile boolean isRunning = false;
    private CountDownLatch startupLatch = new CountDownLatch(1);

    public Acceptor(InetAddressAndPort endPoint, List<Processor> processors, Integer sendBufferSize, Integer recvBufferSize) throws IOException {
        this.endPoint = endPoint;
        this.processors = processors;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
        nioSelector = Selector.open();
        serverChannel = openServerSocket(endPoint);
    }

    protected void startupComplete() {
        isRunning = true;
        startupLatch.countDown();
    }

    public void awaitStartup() throws InterruptedException {
        startupLatch.await();
    }

    public void run() {
        try {
            serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
            startupComplete();
            var currentProcessor = 0;
            while (isRunning) {
                var ready = nioSelector.select(500);
                if (ready > 0) {
                    var keys = nioSelector.selectedKeys();
                    var iter = keys.iterator();
                    while (iter.hasNext() && isRunning) {
                        var key = iter.next();
                        iter.remove();
                        if (key.isAcceptable()) {
                            accept(key, processors.get(currentProcessor));
                        }
                        // round robin to the next processor thread
                        currentProcessor = (currentProcessor + 1) % processors.size();
                    }
                }

            }
        } catch (Exception e) {
            throw new NetworkException(e);
        }
    }

    private void accept(SelectionKey key, Processor processor) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        serverSocketChannel.socket().setReceiveBufferSize(recvBufferSize);

        var socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setKeepAlive(true);
        socketChannel.socket().setSendBufferSize(sendBufferSize);

        getLogger().info("Accepting new connection " + socketChannel.getRemoteAddress() + " localAddress:" + socketChannel.getLocalAddress());

        processor.accept(socketChannel);
    }

    private ServerSocketChannel openServerSocket(InetAddressAndPort address) throws IOException {
        InetSocketAddress socketAddress = new InetSocketAddress(address.getAddress(), address.getPort());
        var serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().setReceiveBufferSize(recvBufferSize);

        try {
            serverChannel.bind(socketAddress);
            getLogger().info("Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.");
        } catch (SocketException e) {
            throw new RuntimeException("Socket server failed to bind to ${socketAddress.getHostString}:$port: ${e.getMessage}.");
        }
        return serverChannel;
    }

}