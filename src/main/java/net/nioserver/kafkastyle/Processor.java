package net.nioserver.kafkastyle;

import common.JsonSerDes;
import common.Logging;
import common.RequestOrResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Processor implements Runnable, Logging {
    private Queue<SocketChannel> newConnections = new ConcurrentLinkedQueue<SocketChannel>();
    protected Selector selector = Selector.open();
    volatile boolean isRunning = false;
    private int id;
    private RequestChannel requestChannel;

    public Processor(int id, RequestChannel requestChannel) throws IOException {
        this.id = id;
        this.requestChannel = requestChannel;
    }

    public void run() {
        isRunning = true;
        while(isRunning) {
            try {
                configureNewConnections();
                processNewResponses();
                var startSelectTime = System.currentTimeMillis();
                var ready = selector.select(300);
                getLogger().trace("Processor id " + id + " selection time = " + (System.currentTimeMillis() - startSelectTime) + " ms");
                if(ready > 0) {
                    var keys = selector.selectedKeys();
                    var iter = keys.iterator();
                    while(iter.hasNext() && isRunning) {
                        SelectionKey key = null;
                        try {
                            key = iter.next();
                            iter.remove();
                            if(key.isReadable())
                                read(key);
                            else if(key.isWritable())
                                write(key);
                            else if(!key.isValid())
                                close(key);
                            else
                                throw new IllegalStateException("Unrecognized key state for processor thread.");
                        } catch(Throwable e) {
                            getLogger().error("Closing socket for " + ((SocketChannel)key.channel()).getRemoteAddress() + " because of error", e);

                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    private void close(SelectionKey key) {
        try {
            var channel = (SocketChannel) key.channel();
            getLogger().debug("Closing connection from " + channel.getRemoteAddress());
            channel.socket().close();
            channel.close();
            key.attach(null);
            key.cancel();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void write(SelectionKey key) throws Exception {
        var socketChannel = (SocketChannel)key.channel();
        ResponseWrapper wrapper = (ResponseWrapper) key.attachment();
        if (wrapper == null) {
            throw new IllegalStateException("Response not attachde to key");
        }
        long bytesRead = wrapper.writeTo(socketChannel);
        if(wrapper.complete) {
            key.attach(null);
            getLogger().trace("Finished writing, registering for read on connection " + socketChannel.getRemoteAddress());
            key.interestOps(SelectionKey.OP_READ);
        } else {
            getLogger().trace("Did not finish writing, registering for write again on connection " + socketChannel.getRemoteAddress());
            key.interestOps(SelectionKey.OP_WRITE);
            wakeup();
        }
    }

    private void read(SelectionKey key) throws Exception {
       var socketChannel = (SocketChannel)key.channel();
       BoundedByteBufferReceive receive  = (BoundedByteBufferReceive)key.attachment();
       if (receive == null) {
           receive = new BoundedByteBufferReceive();
           key.attach(receive);
       }
       int bytesRead = receive.readFrom(socketChannel);
       if (bytesRead < 0) {
           close(key);
       } else if (receive.complete) {
           RequestOrResponse deserialize = JsonSerDes.deserialize(receive.contentBuffer.flip().array(), RequestOrResponse.class);
           RequestOrResponse request = deserialize;
           getLogger().info("Read Request " + request);
           requestChannel.sendRequest(new RequestWrapper(id, request, key));
           key.attach(null);
           // explicitly reset interest ops to not READ, no need to wake up the selector just yet
           key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
       } else {
           // more reading to be done
           getLogger().trace("Did not finish reading, registering for read again on connection " + socketChannel.getRemoteAddress());
           key.interestOps(SelectionKey.OP_READ);
           wakeup();
       }
    }

    private void processNewResponses() {
        var curr = requestChannel.receiveResponse(id);
        while(curr != null) {
            var key = curr.getKey();
            key.interestOps(SelectionKey.OP_WRITE);
            key.attach(curr);
            curr = requestChannel.receiveResponse(id);
        }
    }

    private void configureNewConnections() throws IOException {
        while(newConnections.size() > 0) {
            var channel = newConnections.poll();
            getLogger().debug("Processor " + id + " listening to new connection from " + channel.getRemoteAddress());
            channel.register(selector, SelectionKey.OP_READ);
        }
    }

    public void accept(SocketChannel socketChannel) {
        newConnections.add(socketChannel);
        wakeup();
    }

    private void wakeup() {
        selector.wakeup();
    }
}
