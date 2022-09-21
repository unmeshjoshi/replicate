package distrib.patterns.common;

import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

public abstract class Replica {
    private static Logger logger = LogManager.getLogger(Replica.class);
    private final Config config;
    private final NIOSocketListener peerListener;
    private final NIOSocketListener clientListener;
    private InetAddressAndPort clientConnectionAddress;
    private InetAddressAndPort peerConnectionAddress;
    private final Network network = new Network();

    protected final RequestWaitingList requestWaitingList;
    private List<InetAddressAndPort> peerAddresses;

    public Replica(Config config,
                   SystemClock clock,
                   InetAddressAndPort clientConnectionAddress,
                   InetAddressAndPort peerConnectionAddress,
                   List<InetAddressAndPort> peerAddresses) throws IOException {

        this.config = config;
        this.requestWaitingList = new RequestWaitingList(clock);
        this.peerAddresses = peerAddresses;
        this.clientConnectionAddress = clientConnectionAddress;
        this.peerConnectionAddress = peerConnectionAddress;
        this.peerListener = new NIOSocketListener(this::handleServerMessage, peerConnectionAddress);
        this.clientListener = new NIOSocketListener(this::handleClientRequest, clientConnectionAddress);
    }

    public void start() {
        peerListener.start();
        clientListener.start();
    }

    public void send(InetAddressAndPort fromAddress, RequestOrResponse message) {
        try {
            network.sendOneWay(fromAddress, message);
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + fromAddress);
        }
    }
    public <T> void sendRequestToReplicas(RequestCallback quorumCallback, RequestId requestId, T requestToReplicas) {
        for (InetAddressAndPort replica : peerAddresses) {
            int correlationId = nextRequestId();
            RequestOrResponse request = new RequestOrResponse(requestId.getId(), JsonSerDes.serialize(requestToReplicas), correlationId, getPeerConnectionAddress());
            sendRequestToReplica(quorumCallback, replica, request);
        }
    }

    public void sendRequestToReplica(RequestCallback requestCallback, InetAddressAndPort replicaAddress, RequestOrResponse request) {
        requestWaitingList.add(request.getCorrelationId(), requestCallback);
        send(replicaAddress, request);
    }

    int requestNumber;
    private int nextRequestId() {
        return requestNumber++;
    }

    public abstract void handleClientRequest(Message<RequestOrResponse> message);

    public abstract void handleServerMessage(Message<RequestOrResponse> message);

    public int getNoOfReplicas() {
        return this.peerAddresses.size();
    }

    public InetAddressAndPort getClientConnectionAddress() {
        return clientConnectionAddress;
    }

    public InetAddressAndPort getPeerConnectionAddress() {
        return peerConnectionAddress;
    }

    protected <T> T deserialize(RequestOrResponse request, Class<T> clazz) {
        return JsonSerDes.deserialize(request.getMessageBodyJson(), clazz);
    }

    public void dropMessagesTo(Replica n) {
        network.dropMessagesTo(n.getPeerConnectionAddress());
    }

    public void reconnectTo(Replica n) {
        network.reconnectTo(n.getPeerConnectionAddress());
    }

    public void dropMessagesToAfter(Replica n, int dropAfterNoOfMessages) {
        network.dropMessagesAfter(n.getPeerConnectionAddress(), dropAfterNoOfMessages);
    }

    public void addDelayForMessagesTo(Replica n, int noOfMessages) {
        network.addDelayForMessagesToAfterNMessages(n.getPeerConnectionAddress(), noOfMessages);
    }
}
