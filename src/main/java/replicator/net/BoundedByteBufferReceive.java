package replicator.net;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class BoundedByteBufferReceive {
    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    public ByteBuffer contentBuffer = null;
    public Boolean complete = false;

    public int readFrom(ReadableByteChannel socketChannel) throws IOException {
        expectIncomplete();
        int read = 0;
        if (sizeBuffer.hasRemaining()) {
            int bytesRead = socketChannel.read(sizeBuffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }
        if(contentBuffer == null && !sizeBuffer.hasRemaining()) {
            sizeBuffer.rewind();
            var size = sizeBuffer.getInt();
            contentBuffer = ByteBuffer.allocate(size);
        }
        // if we have a buffer read some stuff into it
        if(contentBuffer != null) {
            read = socketChannel.read(contentBuffer);
            // did we get everything?
            if(!contentBuffer.hasRemaining()) {
                contentBuffer.rewind();
                complete = true;
            }
        }

        return read;
    }

    protected void expectIncomplete() {
        if(complete)
            throw new RuntimeException("This operation cannot be completed on a complete request.");
    }

    protected void expectComplete() {
        if(!complete)
            throw new RuntimeException("This operation cannot be completed on an incomplete request.");
    }
}