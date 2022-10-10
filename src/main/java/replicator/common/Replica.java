package replicator.common;

import replicator.heartbeat.HeartBeatScheduler;
import replicator.net.ClientConnection;
import replicator.net.InetAddressAndPort;
import replicator.net.NIOSocketListener;
import replicator.net.requestwaitinglist.RequestCallback;
import replicator.net.requestwaitinglist.RequestWaitingList;
import replicator.singularupdatequeue.SingularUpdateQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
    The clients communicate with a Replica. For processing client requests, Replicas
    communicate with each other.
    Basic mechanism to support blocking and non-blocking communication
    The blocking communication happens in separate thread handled by blockingExecutor

    All the communication between Replicas is done by message passing.
*/

public abstract class Replica {
    private static Logger logger = LogManager.getLogger(Replica.class);
    private final Config config;
    private final String name;
    private final NIOSocketListener peerListener;
    private final NIOSocketListener clientListener;
    private InetAddressAndPort clientConnectionAddress;
    private InetAddressAndPort peerConnectionAddress;
    private final Network network = new Network();
    protected final RequestWaitingList requestWaitingList;
    protected SystemClock clock;
    private List<InetAddressAndPort> peerAddresses;
    private volatile long heartbeatReceivedNs;

    Map<RequestId, Consumer<Message<RequestOrResponse>>> requestMap = new HashMap<>();

    public Replica(String name, Config config,
                   SystemClock clock,
                   InetAddressAndPort clientConnectionAddress,
                   InetAddressAndPort peerConnectionAddress,
                   List<InetAddressAndPort> peerAddresses) throws IOException {
        this.name = name;

        this.config = config;
        this.requestWaitingList = new RequestWaitingList(clock);
        this.clock = clock;
        this.peerAddresses = peerAddresses;
        this.clientConnectionAddress = clientConnectionAddress;
        this.peerConnectionAddress = peerConnectionAddress;
        this.peerListener = new NIOSocketListener(this::handlePeerMessage, peerConnectionAddress);
        this.clientListener = new NIOSocketListener(this::handleClientRequest, clientConnectionAddress);
        this.registerHandlers();
    }


    /**
     * Following schedulers support implementing basic heartbeat mechanism.
     */
    protected HeartBeatScheduler heartBeatScheduler = new HeartBeatScheduler(()->{
        sendHeartbeats();
    }, 100l); //TODO: Make heartbeat intervals configurable.

    //no-op. implemented by subclass implementations.
    protected void sendHeartbeats() {
        logger.info(getName() + " sending heartbeat message");
    }

    protected Duration heartbeatTimeout = Duration.ofMillis(500);

    protected HeartBeatScheduler heartbeatChecker = new HeartBeatScheduler(()->{
        checkPrimary();
    }, 1000l);

    protected void checkPrimary() {
        //no-op. implemented by implementations.
    }


    public void start() {
        peerListener.start();
        clientListener.start();
        singularUpdateQueue.start();
    }

    //Send message without expecting any messages as a response from the peer
    //@see sendRequestToReplicas which expects a message from the peer.
    public <T extends Request> void sendOneway(InetAddressAndPort address, T request, int correlationId) {
        try {
            network.sendOneWay(address, new RequestOrResponse(request.getRequestId().getId(), serialize(request), correlationId, getPeerConnectionAddress()));
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + address + " from " + getName());
        }
    }

    public <T extends Request> void sendOneway(InetAddressAndPort address, T request) {
        sendOneway(address, request, nextRequestId());
    }

    //Send message to peer and expect a separate message as response.
    //Once the message is received, the callback is invoked.
    //The response message types are configured to invoke responseMessageHandler which invokes the callback
    //@see responseMessageHandler
    public <T> void sendMessageToReplicas(RequestCallback callback, RequestId requestId, T requestToReplicas) {
        for (InetAddressAndPort replica : peerAddresses) {
            int correlationId = nextRequestId();
            RequestOrResponse request = new RequestOrResponse(requestId.getId(), serialize(requestToReplicas), correlationId, getPeerConnectionAddress());
            sendMessageToReplica(callback, replica, request);
        }
    }

    //Sends message to replica and expects that the replica will send back a message with the same correlationId.
    //The message is kept waiting in the RequestWaitingList and expired if the replica fails to send message back.
    public void sendMessageToReplica(RequestCallback callback, InetAddressAndPort replicaAddress, RequestOrResponse request) {
        try {
            logger.debug(getName() + " Sending " + RequestId.valueOf(request.getRequestId()) + " to " + replicaAddress + " with CorrelationId:" + request.getCorrelationId());
            requestWaitingList.add(request.getCorrelationId(), callback);
            network.sendOneWay(replicaAddress, request);
         } catch (IOException e) {
            logger.error("Communication failure sending request to " + replicaAddress + " from " + getName());
            //If communication fails, it should immidiately report it to the callback.
            //Otherwise if a quorum of replica could not be reached, the callback will never complete.
            requestWaitingList.handleError(request.getCorrelationId(), e);
         }
    }

    public <T extends Request> void sendOnewayMessageToReplicas(T requestToReplicas) {
        for (InetAddressAndPort replica : peerAddresses) {
            int correlationId = nextRequestId();
            sendOneway(replica, requestToReplicas, correlationId);
        }
    }

    public <T extends Request> void sendOnewayMessageToOtherReplicas(T requestToReplicas) {
        for (InetAddressAndPort replica : otherReplicas()) {
            int correlationId = nextRequestId();
            sendOneway(replica, requestToReplicas, correlationId);
        }
    }

    private List<InetAddressAndPort> otherReplicas() {
        return peerAddresses.stream().filter(r -> !r.equals(peerConnectionAddress)).collect(Collectors.toList());
    }

    public <Req, Res> List<Res> blockingSendToReplicas(RequestId requestId, Req requestToReplicas) {
        List<Res> responses = new ArrayList<>();
        for (InetAddressAndPort replica : peerAddresses) {
            int correlationId = nextRequestId();
            RequestOrResponse request = new RequestOrResponse(requestId.getId(), serialize(requestToReplicas), correlationId, getPeerConnectionAddress());
            try {
                RequestOrResponse response = network.sendRequestResponse(replica, request);
                Class<Res> responseClass = responseClasses.get(RequestId.valueOf(response.getRequestId()));
                Res res = JsonSerDes.deserialize(response.getMessageBodyJson(), responseClass);
                responses.add(res);
            } catch (IOException e) {
                logger.error(e);
            }
        }
        return responses;
    }

    SingularUpdateQueue<Message<RequestOrResponse>, Void> singularUpdateQueue = new SingularUpdateQueue<Message<RequestOrResponse>, Void>((message) -> {
        markHeartbeatReceived(); //TODO: Mark heartbeats in message handlings explcitily. As this can be user request as well.
        RequestOrResponse request = message.getRequest();
        Consumer consumer = requestMap.get(RequestId.valueOf(request.getRequestId()));
        consumer.accept(message);
        return null;
    });

    //handles messages sent by peers in the cluster in message passing style.
    //peer to peer communication happens on peerConnectionAddress
    public void handlePeerMessage(Message<RequestOrResponse> message) {
        singularUpdateQueue.submit(message);
    }

    protected void markHeartbeatReceived() {
        heartbeatReceivedNs = clock.nanoTime();
    }

    //handles requests sent by clients of the cluster.
    //rpc requests are sent by clients on the clientConnectionAddress
    public void handleClientRequest(Message<RequestOrResponse> message) {
        singularUpdateQueue.submit(message);
    }

    //Configures a handler to process a message.
    //Sends the response from the handler as a message to the sender.
    //This is async message-passing communication.
    //The sender does not expect a response to the request on the same connection.
    //deserialize.andThen(handler.apply).andThen(sendResponseToPeer)
    public <Req extends Request, Res extends Request> ResponseMessageBuilder<Res> handlesMessage(RequestId requestId, Function<Req, Res> handler, Class<Req> requestClass) {
        var deserialize = createDeserializer(requestClass);
        var applyHandler = wrapHandler(handler);
        requestMap.put(requestId, (message)->{
            deserialize
                    .andThen(applyHandler)
                    .andThen(sendMessageToSender)
                    .apply(message);
        });
        return new ResponseMessageBuilder<Res>();
    }

    public <Req extends Request> void handlesMessage(RequestId requestId, BiConsumer<InetAddressAndPort, Req> handler, Class<Req> requestClass) {
        var deserialize = createDeserializer(requestClass);
        Function<Stage<Req>, Void> applyHandler = wrapConsumer(handler);
        requestMap.put(requestId, (message)->{
            deserialize
                    .andThen(applyHandler)
                    .apply(message);
        });
    }

    public class SyncBuilder<T extends Request> {
        public Replica respondsWith(RequestId requestId, Class<T> responseClass) {
            Replica.this.respondsWith(requestId, responseClass);
            return Replica.this;
        }
    }

    public class ResponseMessageBuilder<T extends Request> {
         public Replica respondsWithMessage(RequestId requestId, Class<T> responseClass) {
            Replica.this.respondsWithMessage(requestId, responseClass);
            return Replica.this;
        }
    }

    //Configures a handler to process a given request.
    //Sends response from the handler to the sender.
    //This is request-response  communication or rpc.
    //The sender expects a response to the request on the same connection.
    public <T  extends Request, Res> Replica handlesRequestAsync(RequestId requestId, Function<T, CompletableFuture<Res>> handler, Class<T> requestClass) {
        Function<Message<RequestOrResponse>, Stage<T>> deserialize = createDeserializer(requestClass);
        var handleAsync = asyncWrapHandler(handler);
        requestMap.put(requestId, (message)-> {
            deserialize
                    .andThen(handleAsync)
                    .andThen(asyncRespondToSender)
                    .apply(message);
        });
        return this;
    }


    private Executor blockingExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private Map<RequestId, Class> responseClasses = new HashMap();
    public <T  extends Request, Res extends Request> SyncBuilder<Res> handlesRequestBlocking(RequestId requestId, Function<T, Res> handler, Class<T> requestClass) {
        Function<Message<RequestOrResponse>, Stage<T>> deserialize = createDeserializer(requestClass);
        var handleSync = wrapHandler(handler);
        requestMap.put(requestId, (message)-> {
            blockingExecutor.execute(() -> {
            try {
                deserialize
                        .andThen(handleSync)
                        .andThen(syncRespondToSender)
                        .apply(message);
            } catch(Exception e) {
                RequestOrResponse request = message.getRequest();
                message.getClientConnection().write(new RequestOrResponse(request.getRequestId(), serialize(e.getMessage()), request.getCorrelationId()).setError());
            }
            });
        });
        return new SyncBuilder<Res>();
    }

    public void respondsWith(RequestId id, Class clazz) {
        responseClasses.put(id, clazz);
    }

    //Configures a handler to process a message from the peer in response to the message this peer has sent.
    //@see responseHandler and sendRequestToReplicas
    private <T extends Request> void respondsWithMessage(RequestId requestId, Class<T> responseClass) {
        Function<Message<RequestOrResponse>, Stage<T>> deserializer = createDeserializer(responseClass);
        requestMap.put(requestId, (message) -> {
            deserializer.andThen(responseHandler).apply(message);
        }); //class is not used for deserialization for responses.
    }


    Function<Stage, Void> sendMessageToSender = stage -> {
        Message<RequestOrResponse> message = stage.getMessage();
        Replica.this.sendOneway(message.getRequest().getFromAddress(), stage.request, message.getRequest().getCorrelationId());
        return null;
    };


    Function<Stage, Void> syncRespondToSender = (stage) -> {
        var response = stage.getRequest();
        Message<RequestOrResponse> message = stage.getMessage();
        RequestOrResponse request = (RequestOrResponse) stage.getMessage().getRequest();
        var correlationId = request.getCorrelationId();
        ClientConnection clientConnection = message.getClientConnection();
        clientConnection.write(new RequestOrResponse(response.getRequestId().getId(),
                                                serialize(response), correlationId));
        return null;
    };

    Function<AsyncStage, Void> asyncRespondToSender = (stage) -> {
        CompletableFuture<?> responseFuture = stage.getRequest();
        Message<RequestOrResponse> message = stage.getMessage();
        RequestOrResponse request = (RequestOrResponse) stage.getMessage().getRequest();
        var correlationId = request.getCorrelationId();
        responseFuture.whenComplete((res , throwable)-> {
            ClientConnection clientConnection = message.getClientConnection();
            if (throwable != null) {
                clientConnection.write(new RequestOrResponse(request.getRequestId(), JsonSerDes.serialize(throwable.getMessage()), correlationId).setError());
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

    private <Req extends Request, Void> Function<Stage<Req>, Void> wrapConsumer(BiConsumer<InetAddressAndPort, Req> handler) {
        Function<Stage<Req>, Void> applyHandler = (stage) -> {
            handler.accept(stage.getMessage().getRequest().getFromAddress(), (Req) stage.request);
            return null;
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

    public void dropAfterNMessagesTo(Replica n, int dropAfterNoOfMessages) {
        network.dropMessagesAfter(n.getPeerConnectionAddress(), dropAfterNoOfMessages);
    }

    public void addDelayForMessagesTo(Replica n, int noOfMessages) {
        network.addDelayForMessagesToAfterNMessages(n.getPeerConnectionAddress(), noOfMessages);
    }

    public int quorum() {
        return getNoOfReplicas() / 2 + 1;
    }

    private static byte[] serialize(Object e) {
        return JsonSerDes.serialize(e);
    }

    private <Req extends Request> Req deserialize(Class<Req> requestClass, RequestOrResponse request) {
        return JsonSerDes.deserialize(request.getMessageBodyJson(), requestClass);
    }

    protected abstract void registerHandlers();

    public void shutdown() {
        peerListener.shudown();
        clientListener.shudown();
        singularUpdateQueue.shutdown();
    }

    public Duration elapsedTimeSinceLastHeartbeat() {
        return Duration.ofNanos(clock.nanoTime() - heartbeatReceivedNs);
    }


    public void resetHeartbeat(long heartbeatReceivedNs) {
        this.heartbeatReceivedNs = heartbeatReceivedNs;
    }

    public String getName() {
        return name;
    }
}
