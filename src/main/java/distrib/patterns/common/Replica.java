package distrib.patterns.common;

import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

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

    public static class RequestDetails<T extends Message<RequestOrResponse>> {
        Consumer<T> handler;
        Class requestClass;

        public RequestDetails(Consumer<T> handler, Class requestClass) {
            this.handler = handler;
            this.requestClass = requestClass;
        }
    }

    Map<RequestId, RequestDetails> requestMap = new HashMap<>();

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

    //TODO:Check why its needed to send the peer address.
    public <T> void sendOneway(InetAddressAndPort address, RequestId id, T request, int correlationId) {
        send(address, new RequestOrResponse(id.getId(), JsonSerDes.serialize(request), correlationId, getPeerConnectionAddress()) );
    }

    public void send(InetAddressAndPort address, RequestOrResponse message) {
        try {
            network.sendOneWay(address, message);
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + address);
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


    public void handleServerMessage(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        RequestDetails requestDetails = requestMap.get(RequestId.valueOf(request.getRequestId()));
        requestDetails.handler.accept(message);
    }

    public <T> void registerResponse(RequestId requestId, Class<T> responseClass) {
        requestMap.put(requestId, new RequestDetails<>((message) -> {
            RequestOrResponse response = message.getRequest();
            T res = JsonSerDes.deserialize(response.getMessageBodyJson(), responseClass);
            requestWaitingList.handleResponse(response.getCorrelationId(), res, response.fromAddress);
        }, Void.class)); //class is not used for deserialization for responses.
    }

    public <Req extends Request, Res extends Request> void register(RequestId requestId, Function<Req, Res> handler, Class<Req> requestClass) {
        requestMap.put(requestId, new RequestDetails<>((message) -> {
            RequestOrResponse request = message.getRequest();
            Req r = JsonSerDes.deserialize(request.getMessageBodyJson(), requestClass);
            Request response = (Request) handler.apply(r);
            if (response != null) {
                sendOneway(request.getFromAddress(), response.getRequestId(), response, request.getCorrelationId());
            }
        }
                , requestClass));
    }

    public static class Response<T> {
        Exception e;
        T result;

        public Response(Exception e) {
            this.e = e;
        }

        public Response(T result) {
            this.result = result;
        }

        public Exception getError() {
            return e;
        }

        public T getResult() {
            return result;
        }

        public boolean isErrorResponse() {
            return e != null;
        }

        //jackson.
        private Response() {
        }
    }

    public <T  extends Request, Res> void registerClientRequest(RequestId requestId, Function<T, CompletableFuture<Res>> handler, Class<T> requestClass) {
        requestMap.put(requestId, new RequestDetails<Message<RequestOrResponse>>((message)-> {
            RequestOrResponse request = message.getRequest();
            T r = JsonSerDes.deserialize(request.getMessageBodyJson(), requestClass);
            CompletableFuture<Res> response = handler.apply(r);
            response.whenComplete((res , e)-> {
                if (e != null) {
                    message.getClientConnection().write(new RequestOrResponse(requestId.getId(), JsonSerDes.serialize(e), request.getCorrelationId()).setError());
                } else {
                    message.getClientConnection().write(new RequestOrResponse(requestId.getId(), JsonSerDes.serialize(res), request.getCorrelationId()));
                }
            });
        }
        , requestClass));
    }

    int requestNumber;
    private int nextRequestId() {
        return new Random().nextInt();
    }

    public void handleClientRequest(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        RequestDetails requestDetails = requestMap.get(RequestId.valueOf(request.getRequestId()));
        requestDetails.handler.accept(message);
    }

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
