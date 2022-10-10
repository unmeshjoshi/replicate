package replicator.net;

import distrib.patterns.common.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import replicator.common.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class NIOConnection implements ClientConnection, Logging {
    private static final Logger LOG = LogManager.getLogger(NIOConnection.class);

    private SocketChannel sock;
    private SelectionKey sk;
    private NIOSocketListener server;
    private boolean closed;
    private BoundedByteBufferReceive receive = null;
    LinkedBlockingQueue<ByteBuffer> outgoingBuffers = new LinkedBlockingQueue<ByteBuffer>();
    private RequestConsumer requestConsumer;
    public NIOConnection(SocketChannel sock, SelectionKey sk, NIOSocketListener server, RequestConsumer consumer) {
        this.sock = sock;
        this.sk = sk;
        this.server = server;
        this.requestConsumer = consumer;
    }

    void doIO(SelectionKey selectionKey) throws InterruptedException {
        try {
            if (sock == null) {
                return;
            }
            if (selectionKey.isReadable()) {
                read(selectionKey);
            }
            //<codeFragment name="nioWrite">
            if (selectionKey.isWritable()) {
                if (outgoingBuffers.size() > 0) {
                    long bytesSent = sock.write(outgoingBuffers.toArray(ByteBuffer[]::new));
                    for (ByteBuffer outgoingBuffer : outgoingBuffers) {
                        if (!outgoingBuffer.hasRemaining()) {
                            outgoingBuffers.remove(outgoingBuffer);
                        }
                    }
                    if (outgoingBuffers.isEmpty()) {
                        selectionKey.interestOps(selectionKey.interestOps() & (~SelectionKey.OP_WRITE));
                    }
                }
            }
            //</codeFragment>
        } catch (CancelledKeyException e) {
            close();
        } catch (Exception e) {
            // LOG.error("FIXMSG",e);
            close();
        }
    }

    Executor executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private void read(SelectionKey key) throws Exception {
        var socketChannel = (SocketChannel)key.channel();
        if (receive == null) {
            receive = new BoundedByteBufferReceive();
        }
        int bytesRead = receive.readFrom(socketChannel);
        if (bytesRead < 0) {
            close();
        } else if (receive.complete) {
            RequestOrResponse deserialize = JsonSerDes.deserialize(receive.contentBuffer.flip().array(), RequestOrResponse.class);
            RequestOrResponse request = deserialize;

            //TODO:submit request
            //requestChannel.sendRequest(new RequestWrapper(id, request, key));
            RequestId requestId = RequestId.valueOf(request.getRequestId());
            //submit for execution.
            executor.execute(()-> requestConsumer.accept(new Message<RequestOrResponse>(request, request.getGeneration(), requestId, this)));
            receive = null; //ready to read next request.
        } else {
            // more reading to be done
            getLogger().trace("Did not finish reading, registering for read again on connection " + socketChannel.getRemoteAddress());
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        synchronized (server.cnxns) {
            server.cnxns.remove(this);
        }
        if (requestConsumer != null) {
            requestConsumer.close(this);
        }


        try {
            /*
             * The following sequence of code is stupid! You would think that
             * only sock.close() is needed, but alas, it doesn't work that way.
             * If you just do sock.close() there are cases where the socket
             * doesn't actually close...
             */
            sock.socket().shutdownOutput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
        }
        try {
            sock.socket().shutdownInput();
        } catch (IOException e) {
            LOG.warn("ignoring exception during input shutdown", e);
        }
        try {
            sock.socket().close();
        } catch (IOException e) {
            LOG.warn("ignoring exception during socket close", e);
        }
        try {
            sock.close();
            // XXX The next line doesn't seem to be needed, but some posts
            // to forums suggest that it is needed. Keep in mind if errors in
            // this section arise.
            // factory.selector.wakeup();
        } catch (IOException e) {
            LOG.warn("ignoring exception during socketchannel close", e);
        }
        sock = null;
        if (sk != null) {
            try {
                // need to cancel this selection key from the selector
                sk.cancel();
            } catch (Exception e) {
                LOG.warn("ignoring exception during selectionkey cancel", e);
            }
        }
    }

    //<codeFragment name="nioClientConnectionWrite">
    @Override
    public void write(RequestOrResponse response) {
        ByteBuffer responseBuffer = serializeResponse(response);
        outgoingBuffers.add(responseBuffer);
        sk.selector().wakeup();
        if (sk.isValid()) {
            sk.interestOps(sk.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private ByteBuffer serializeResponse(RequestOrResponse response) {
        byte[] serializedResponse = JsonSerDes.serialize(response);
        ByteBuffer responseBuffer = ByteBuffer.allocate(4 + serializedResponse.length);
        responseBuffer.putInt(serializedResponse.length);
        responseBuffer.put(serializedResponse);
        responseBuffer.flip();
        return responseBuffer;
    }
    //</codeFragment>
}
