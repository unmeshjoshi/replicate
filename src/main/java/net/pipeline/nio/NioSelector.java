package net.pipeline.nio;


import common.JsonSerDes;
import common.RequestOrResponse;
import net.InetAddressAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class NioSelector {
    static Logger logger = LogManager.getLogger(NioSelector.class);

    private LinkedBlockingDeque<SocketRequestOrResponse> requests = new LinkedBlockingDeque<>();
    private Map<InetAddressAndPort, Channel> channels = new HashMap<>();
    private final Selector selector;
    private Consumer<SocketResponse> consumer;

    public NioSelector(Consumer<SocketResponse> consumer) throws IOException {
        this.consumer = consumer;
        selector = Selector.open();
    }


    public void initConnectionAndQueueRequest(InetAddressAndPort address, RequestOrResponse request) {
        try {
            Channel channel = channels.get(address);
            if (channel == null || !channel.getSocketChannel().isConnected()) {
                connect(address);
            }
            requests.put(new SocketRequestOrResponse(address, request));
            logger.info("New request queued, waking up selector"); //wakeup selector thread, so that send process gets executed
            selector.wakeup();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void connect(InetAddressAndPort address) throws IOException {
        Channel channel = channels.get(address);
        if (channel == null) {
            channel = initConnection(address);
            channels.put(address, channel);
        }
    }

    public Channel initConnection(InetAddressAndPort address) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        configureSocketChannel(socketChannel, -1, -1);
        boolean connect = socketChannel.connect(new InetSocketAddress(address.getAddress(), address.getPort()));
        SelectionKey selectionKey = registerChannel(socketChannel, SelectionKey.OP_CONNECT);
        Channel channel = new Channel(selectionKey, address, socketChannel);
        selectionKey.attach(channel);
        return channel;
    }

    private SelectionKey registerChannel(SocketChannel socketChannel, int interestedOps) throws IOException {
        SelectionKey key = socketChannel.register(selector, interestedOps);
        selector.wakeup();
        return key;
    }


    private void configureSocketChannel(SocketChannel socketChannel, int sendBufferSize, int receiveBufferSize)
            throws IOException {
        socketChannel.configureBlocking(false);
        Socket socket = socketChannel.socket();
        socket.setKeepAlive(true);
        socket.setSendBufferSize(1024);
        socket.setReceiveBufferSize(1024);
        socket.setTcpNoDelay(true);
    }

    public void poll() throws IOException {
        processSends();
        long before = System.nanoTime();
        int numReadyKeys = selector.select(200);
        long after = System.nanoTime();
        logger.info("selected after = " + TimeUnit.NANOSECONDS.toMillis(after - before) + " ms.");
        handleSelections();
    }

    private void processSends() {
        if (requests.isEmpty()) {
            return;
        }
        SocketRequestOrResponse socketRequestOrResponse = requests.getFirst();
        try {
            connectIfNeeded(socketRequestOrResponse);

        } catch (IOException e) {
            logger.error("Unable to connect to " + socketRequestOrResponse.address + ". Removing the request.");
            logger.error(e);
            requests.remove(socketRequestOrResponse);
        }

        Channel channel = channels.get(socketRequestOrResponse.address);
        if (channel.canSend()) {
            channel.send(socketRequestOrResponse.getRequest());
            channel.getKey().interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
            logger.info("Waking up selector");
            selector.wakeup();
            requests.remove(socketRequestOrResponse);
        }

    }

    private void connectIfNeeded(SocketRequestOrResponse socketRequestOrResponse) throws IOException {
        Channel channel = channels.get(socketRequestOrResponse.address);
        if (channel == null) {
            logger.info("Not connected to " + socketRequestOrResponse.address + ". Initializing connection");
            connect(socketRequestOrResponse.address);
        }
    }

    private void handleSelections() {
        Set<SelectionKey> readyKeys = selector.selectedKeys();
        if (readyKeys.size() > 0) {
            logger.info("numReadyKeys = " + readyKeys.size());
        }
        for (SelectionKey selectionKey : readyKeys) {
            try {
                handleIO(selectionKey);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        readyKeys.clear();
    }

    private void handleIO(SelectionKey selectionKey) throws IOException {
        Channel channel = (Channel) selectionKey.attachment();
        if (channel == null) {
            return;
        }
        try {
            SocketChannel socketChannel = channel.getSocketChannel();
            if (selectionKey.isConnectable()) {
                boolean connected = socketChannel.finishConnect();
                if (connected)
                    selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);

            }

            if (selectionKey.isWritable()) {
                int write = channel.write();
                if (channel.canSend()) {
                    //disable write interest if send is complete.
                    selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_WRITE | SelectionKey.OP_READ);
                }
            }

            if (selectionKey.isReadable()) {
                ByteBuffer read = channel.read();
                if (read != null) {
                    RequestOrResponse deserialize = JsonSerDes.deserialize(read.array(), RequestOrResponse.class);
                    consumer.accept(new SocketResponse(channel.getAddress(), deserialize));
                }
            }
        } catch (IOException e) {
            logger.error("Exception handling request to " + channel.getAddress() + ". Removing from connection list");
            channel.close();
            channels.remove(channel.getAddress());
        }
    }

    public void stop() {
        try {
            selector.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
