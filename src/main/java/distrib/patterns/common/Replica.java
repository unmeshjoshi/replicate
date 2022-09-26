package distrib.patterns.common;

import distrib.patterns.net.ClientConnection;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
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

    //Send message without expecting any messages as a response from the peer
    //@see sendRequestToReplicas which expects a message from the peer.
    //TODO:Check why its needed to send the peer address.
    public <T> void sendOneway(InetAddressAndPort address, RequestId id, T request, int correlationId) {
        send(address, new RequestOrResponse(id.getId(), serialize(request), correlationId, getPeerConnectionAddress()) );
    }

    public void send(InetAddressAndPort address, RequestOrResponse message) {
        try {
            network.sendOneWay(address, message);
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + address);
        }
    }

    //Send message to peer and expect a separate message as response.
    //Once the message is received, the callback is invoked.
    //The response message types are configured to invoke responseMessageHandler which invokes the callback
    //@see responseMessageHandler
    public <T> void sendRequestToReplicas(RequestCallback callback, RequestId requestId, T requestToReplicas) {
        for (InetAddressAndPort replica : peerAddresses) {
            int correlationId = nextRequestId();
            RequestOrResponse request = new RequestOrResponse(requestId.getId(), serialize(requestToReplicas), correlationId, getPeerConnectionAddress());
            sendRequestToReplica(callback, replica, request);
        }
    }

    public void sendRequestToReplica(RequestCallback callback, InetAddressAndPort replicaAddress, RequestOrResponse request) {
        requestWaitingList.add(request.getCorrelationId(), callback);
        send(replicaAddress, request);
    }


    public <T> List<RequestOrResponse> blockingSendToReplicas(RequestId requestId, T requestToReplicas) {
        List<RequestOrResponse> responses = new ArrayList<>();
        for (InetAddressAndPort replica : peerAddresses) {
            int correlationId = nextRequestId();
            RequestOrResponse request = new RequestOrResponse(requestId.getId(), serialize(requestToReplicas), correlationId, getPeerConnectionAddress());
            try {
                RequestOrResponse response = network.sendRequestResponse(replica, request);
                responses.add(response);
            } catch (IOException e) {
                logger.error(e);
            }
        }
        return responses;
    }

    //handles messages sent by peers in the cluster in message passing style.
    //peer to peer communication happens on peerConnectionAddress
    public void handlePeerMessage(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        Consumer consumer = requestMap.get(RequestId.valueOf(request.getRequestId()));
        consumer.accept(message);
    }

    //handles requests sent by clients of the cluster.
    //rpc requests are sent by clients on the clientConnectionAddress
    public void handleClientRequest(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        Consumer consumer = requestMap.get(RequestId.valueOf(request.getRequestId()));
        consumer.accept(message);
    }

    //Configures a handler to process a message.
    //Sends the response from the handler as a message to the sender.
    //This is async message-passing communication.
    //The sender does not expect a response to the request on the same connection.
    //deserialize.andThen(handler.apply).andThen(sendResponseToPeer)
    public <Req extends Request, Res extends Request> Replica messageHandler(RequestId requestId, Function<Req, Res> handler, Class<Req> requestClass) {
        var deserialize = createDeserializer(requestClass);
        var applyHandler = wrapHandler(handler);
        requestMap.put(requestId, (message)->{
            deserialize.andThen(applyHandler).andThen(sendMessageToSender).apply(message);
        });
        return this;
    }

    //Configures a handler to process a given request.
    //Sends response from the handler to the sender.
    //This is request-response  communication or rpc.
    //The sender expects a response to the request on the same connection.
    public <T  extends Request, Res> void requestHandler(RequestId requestId, Function<T, CompletableFuture<Res>> handler, Class<T> requestClass) {
        Function<Message<RequestOrResponse>, Stage<T>> deserializer = createDeserializer(requestClass);
        var asyncHandler = asyncWrapHandler(handler);
        requestMap.put(requestId, (message)-> {
            deserializer.andThen(asyncHandler).andThen(respondToSender).apply(message);
        });
    }

    //Configures a handler to process a message from the peer in response to the message this peer has sent.
    //@see responseHandler and sendRequestToReplicas
    public <T extends Request> void responseMessageHandler(RequestId requestId, Class<T> responseClass) {
        Function<Message<RequestOrResponse>, Stage<T>> deserializer = createDeserializer(responseClass);
        requestMap.put(requestId, (message) -> {
            deserializer.andThen(responseHandler).apply(message);
        }); //class is not used for deserialization for responses.
    }


    Function<Stage, Void> sendMessageToSender = stage -> {
        Message<RequestOrResponse> message = stage.getMessage();
        Replica.this.sendOneway(message.getRequest().getFromAddress(), stage.request.getRequestId(), stage.request, message.getRequest().getCorrelationId());
        return null;
    };

    Function<AsyncStage, Void> respondToSender = (stage) -> {
        var response = stage.getRequest();
        Message<RequestOrResponse> message = stage.getMessage();
        RequestOrResponse request = (RequestOrResponse) stage.getMessage().getRequest();
        var correlationId = request.getCorrelationId();
        response.whenComplete((res , e)-> {
            ClientConnection clientConnection = message.getClientConnection();
            if (e != null) {
                clientConnection.write(new RequestOrResponse(request.getRequestId(), serialize(e), correlationId).setError());
            } else {
                clientConnection.write(new RequestOrResponse(request.getRequestId(), serialize(res), correlationId));
            }
        });
        return null;
    };

    Function<Stage, Void> responseHandler = (stage) -> {
        Message<RequestOrResponse> message = stage.message;
        var response = message.getRequest();
        Replica.this.requestWaitingList.handleResponse(response.getCorrelationId(), stage.request, response.fromAddress);
        return null;
    };

    private <Req extends Request, Res extends CompletableFuture> Function<Stage<Req>, AsyncStage> asyncWrapHandler(Function<Req, Res> handler) {
        return (stage) -> {
            Res response = handler.apply((Req) stage.request);
            return new AsyncStage(stage.getMessage(), response);
        };
    }

    private <Req extends Request, Res extends Request> Function<Stage<Req>, Stage> wrapHandler(Function<Req, Res> handler) {
        Function<Stage<Req>, Stage> applyHandler = (stage) -> {
            Res response = handler.apply((Req) stage.request);
            return new Stage(stage.getMessage(), response);
        };
        return applyHandler;
    }

    private <Req extends Request> Function<Message<RequestOrResponse>, Stage<Req>> createDeserializer(Class<Req> requestClass) {
        Function<Message<RequestOrResponse>, Stage<Req>> deserialize = (message) -> {
            RequestOrResponse request = message.getRequest();
            Req r = deserialize(requestClass, request);
            return new Stage<>(message, r);
        };
        return deserialize;
    }

    private int nextRequestId() {
        return new Random().nextInt();
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

    private static byte[] serialize(Object e) {
        return JsonSerDes.serialize(e);
    }

    private <Req extends Request> Req deserialize(Class<Req> requestClass, RequestOrResponse request) {
        return JsonSerDes.deserialize(request.getMessageBodyJson(), requestClass);
    }
}
