package net.nioserver.kafkastyle;

import net.InetAddressAndPort;

import java.util.ArrayList;
import java.util.List;

public class NIOServer {
    int numProcessorThreads = 2;
    private int maxQueuedRequests = 500;
    private int socketSendBufferBytes = 100*1024;
    private int socketReceiveBufferBytes = 100*1024;
    private int socketRequestMaxBytes = 100*1024*1024;

    private List<Processor> processors = new ArrayList<>(numProcessorThreads);
    RequestChannel requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests);
    private Acceptor acceptor;
    private InetAddressAndPort address;

    public NIOServer(InetAddressAndPort address) {
        this.address = address;
    }

    public void start() throws Exception {
        for (int i = 0; i < numProcessorThreads; i++) {
            Processor p = new Processor(i, requestChannel);
            processors.add(p);
            new Thread(p).start();
        }
        this.acceptor = new Acceptor(address, processors, socketSendBufferBytes, socketReceiveBufferBytes);
        this.acceptor.start();
        acceptor.awaitStartup();

        new NioRequestHandler(requestChannel).start();
    }

}
