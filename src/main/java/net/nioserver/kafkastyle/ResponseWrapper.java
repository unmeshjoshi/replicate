package net.nioserver.kafkastyle;

import common.JsonSerDes;
import common.RequestOrResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ResponseWrapper {
    public boolean complete = false;
    private int processorId;
    private SelectionKey key;
    private RequestOrResponse response;
    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    ByteBuffer responseBuffer;

    public ResponseWrapper(int processorId, SelectionKey key, RequestOrResponse response) {
        this.processorId = processorId;
        this.key = key;
        this.response = response;
        byte[] serialize = JsonSerDes.serialize(response);
        sizeBuffer.putInt(serialize.length);
        sizeBuffer.rewind();
        responseBuffer = ByteBuffer.allocate(serialize.length);
        responseBuffer.put(serialize);
        responseBuffer.rewind();
    }

    public int getProcessorId() {
        return processorId;
    }

    public SelectionKey getKey() {
        return key;
    }

    public RequestOrResponse getResponse() {
        return response;
    }

    public long writeTo(SocketChannel socketChannel) throws IOException {
        long written = socketChannel.write(new ByteBuffer[]{sizeBuffer, responseBuffer});
        if (!sizeBuffer.hasRemaining() &&  !responseBuffer.hasRemaining()) {
            complete = true;
        }
        return written;
    }
}
