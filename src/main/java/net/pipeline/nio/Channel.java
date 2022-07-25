package net.pipeline.nio;


import common.JsonSerDes;
import common.RequestOrResponse;
import net.InetAddressAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

class Channel {
    private static Logger logger = LogManager.getLogger(Channel.class);
    static int sizeOfInt = 4;
    private InetAddressAndPort address;
    private SelectionKey key;
    private SocketChannel socketChannel;
    private int requestedBufferSize = -1;
    private ByteBuffer requestRead = null;
    private ByteBuffer size = ByteBuffer.allocate(sizeOfInt);

    private ByteBuffer responseToWrite;
    private int remaining;

    public Channel(SelectionKey key, InetAddressAndPort address, SocketChannel socketChannel) {
        this.key = key;
        this.address = address;
        this.socketChannel = socketChannel;
    }

    public SelectionKey getKey() {
        return key;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public int write() throws IOException {
        int wrote = 0;
        if (responseToWrite != null) {
            wrote = socketChannel.write(responseToWrite);
            remaining -= wrote;
        }
        if (responseToWrite != null && remaining == 0) {
            responseToWrite = null;
        }
        return remaining;
    }

    public boolean hasPendingWrites() {
        return remaining > 0;
    }

    public void maybeCompleteSend() {
        if (responseToWrite != null && !responseToWrite.hasRemaining()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            ByteBuffer result = responseToWrite;
            responseToWrite = null;
        }
    }

    public ByteBuffer read() throws IOException {
        int read = 0;
        if (size.hasRemaining()) {
            int bytesRead = socketChannel.read(size);
            if (bytesRead < 0)
                return null;
            read += bytesRead;
            if (!size.hasRemaining()) {
                size.flip();
                int receiveSize = size.getInt();
                requestedBufferSize = receiveSize; //may be 0 for some payloads (SASL)
            }
        }
        if (requestRead == null && requestedBufferSize != -1) { //we know the size we want but havent been able to allocate it yet
            logger.info("reading " + requestedBufferSize + " bytes");
            requestRead = ByteBuffer.allocate(requestedBufferSize);
        }
        if (requestRead != null) {
            int bytesRead = socketChannel.read(requestRead);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }
        return maybeCompleteReceive();
    }

    public ByteBuffer maybeCompleteReceive() {
        if (requestRead != null && !requestRead.hasRemaining()) {
            size.clear();
            requestRead.rewind();
            ByteBuffer result = requestRead;
            requestRead = null;
            requestedBufferSize = -1;
            return result;
        }
        return null;
    }

    public boolean isConnectable() {
        return key.isConnectable();
    }


    public boolean canSend() {
        return socketChannel.isConnected() && !hasPendingWrites();
    }

    public void send(RequestOrResponse response) {
        byte[] bytes = JsonSerDes.serialize(response);
        responseToWrite = ByteBuffer.allocate(sizeOfInt + bytes.length);
        responseToWrite.putInt(bytes.length);
        responseToWrite.put(bytes);
        remaining = sizeOfInt + bytes.length;
        responseToWrite.flip();
    }

    public void close() {
        try {
            socketChannel.close();
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public InetAddressAndPort getAddress() {
        return address;
    }
}
