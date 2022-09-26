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

/*

 */

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


    Map<RequestId, Consumer<Message<RequestOrResponse>>> requestMap = new HashMap<>();

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
        this.peerListener = new NIOSocketListener(this::handlePeerMessage, peerConnectionAddress);
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


    public void handlePeerMessage(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        Consumer consumer = requestMap.get(RequestId.valueOf(request.getRequestId()));
        consumer.accept(message);
    }

    public <T extends Request> void responseMessageHandler(RequestId requestId, Class<T> responseClass) {
        Function<Message<RequestOrResponse>, Stage<T>> deserializer = createDeserializer(responseClass);
        requestMap.put(requestId, (message) -> {
            deserializer.andThen(responseHandler).apply(message);
        }); //class is not used for deserialization for responses.
    }

    static class Stage<Req extends Request> {
        private Message<RequestOrResponse> message;
        private Req request;

        public Stage(Message<RequestOrResponse> message, Req request) {
            this.message = message;
            this.request = request;
        }

        public Req getRequest() {
            return request;
        }

        public Message<RequestOrResponse> getMessage() {
            return message;
        }
    }

    static class AsyncStage<Req> {
        private Message<RequestOrResponse> message;
        private CompletableFuture<Req> request;

        public AsyncStage(Message<RequestOrResponse> message, CompletableFuture<Req> request) {
            this.message = message;
            this.request = request;
        }

        public CompletableFuture<Req> getRequest() {
            return request;
        }

        public Message<RequestOrResponse> getMessage() {
            return message;
        }
    }

    Function<Stage, Void> respondToPeer = req -> {
        Message<RequestOrResponse> message = req.getMessage();
        sendOneway(message.getRequest().getFromAddress(), req.request.getRequestId(), req.request, message.getRequest().getCorrelationId());
        return null;
    };

    Function<Stage, Void> responseHandler = (stage) -> {
        Message<RequestOrResponse> message = stage.message;
        var response = message.getRequest();
        Replica.this.requestWaitingList.handleResponse(response.getCorrelationId(), stage.request, response.fromAddress);
        return null;
    };
    //deserialize.andThen(handler.apply).andThen(sendResponseToPeer)
    public <Req extends Request, Res extends Request> Replica messageHandler(RequestId requestId, Function<Req, Res> handler, Class<Req> requestClass) {
        var deserialize = createDeserializer(requestClass);
        var applyHandler = wrapHandler(handler);

        requestMap.put(requestId, (message)->{
            deserialize.andThen(applyHandler).andThen(respondToPeer).apply(message);
        });
        return this;
    }

    private static <Req extends Request, Res extends CompletableFuture> Function<Stage<Req>, AsyncStage> asyncWrapHandler(Function<Req, Res> handler) {
        return (stage) -> {
            Res response = handler.apply((Req) stage.request);
            return new AsyncStage(stage.getMessage(), response);
        };
    }

    private static <Req extends Request, Res extends Request> Function<Stage<Req>, Stage> wrapHandler(Function<Req, Res> handler) {
        Function<Stage<Req>, Stage> applyHandler = (stage) -> {
            Res response = handler.apply((Req) stage.request);
            return new Stage(stage.getMessage(), response);
        };
        return applyHandler;
    }

    private static <Req extends Request> Function<Message<RequestOrResponse>, Stage<Req>> createDeserializer(Class<Req> requestClass) {
        Function<Message<RequestOrResponse>, Stage<Req>> deserialize = (message) -> {
            RequestOrResponse request = message.getRequest();
            Req r = JsonSerDes.deserialize(request.getMessageBodyJson(), requestClass);
            return new Stage<>(message, r);
        };
        return deserialize;
    }

    public <T  extends Request, Res> void requestHandler(RequestId requestId, Function<T, CompletableFuture<Res>> handler, Class<T> requestClass) {
        Function<Message<RequestOrResponse>, Stage<T>> deserializer = createDeserializer(requestClass);
        var asyncHandler = asyncWrapHandler(handler);
        requestMap.put(requestId, (message)-> {
            deserializer.andThen(asyncHandler).andThen(respondToClient).apply(message);
        });
    }

    Function<AsyncStage, Void> respondToClient = (stage) -> {
        var response = stage.getRequest();
        Message<RequestOrResponse> message = stage.getMessage();
        RequestOrResponse request = (RequestOrResponse) stage.getMessage().getRequest();
        var correlationId = request.getCorrelationId();
        response.whenComplete((res , e)-> {
            if (e != null) {
                message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(e), correlationId).setError());
            } else {
                message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(res), correlationId));
            }
        });
        return null;
    };

    private static <Res extends Request> void respondToRequest(Message<RequestOrResponse> message, CompletableFuture<Res> response, Integer correlationId) {
        response.whenComplete((res , e)-> {
            if (e != null) {
                message.getClientConnection().write(new RequestOrResponse(res.getRequestId().getId(), JsonSerDes.serialize(e), correlationId).setError());
            } else {
                message.getClientConnection().write(new RequestOrResponse(res.getRequestId().getId(), JsonSerDes.serialize(res), correlationId));
            }
        });
    }

    private int nextRequestId() {
        return new Random().nextInt();
    }

    public void handleClientRequest(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        Consumer consumer = requestMap.get(RequestId.valueOf(request.getRequestId()));
        consumer.accept(message);
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
