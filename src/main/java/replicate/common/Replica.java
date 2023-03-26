package replicate.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.heartbeat.HeartBeatScheduler;
import replicate.net.ClientConnection;
import replicate.net.InetAddressAndPort;
import replicate.net.NIOSocketListener;
import replicate.net.requestwaitinglist.RequestCallback;
import replicate.net.requestwaitinglist.RequestWaitingList;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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
    private volatile long heartbeatReceivedNs = 0;

    //SingleThreaded executor used to execute all the state manipulation methods of replica, so that
    //all the state updates happen in a single thread, without needing any synchronization.
    protected ScheduledExecutorService singularUpdateQueueExecutor = Executors.newSingleThreadScheduledExecutor();

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


    //TODO: Make heartbeat intervals configurable.
    private final Duration heartBeatInterval = Duration.ofMillis(100l);

    /**
     * Following schedulers support implementing basic heartbeat mechanism.
     */
    protected HeartBeatScheduler heartBeatScheduler = new HeartBeatScheduler(()->{
        sendHeartbeats();
    }, heartBeatInterval.toMillis());

    //no-op. implemented by subclass implementations.
    protected void sendHeartbeats() {
        logger.info(getName() + " sending heartbeat message");
    }

    protected Duration heartbeatTimeout = Duration.ofMillis(heartBeatInterval.toMillis() * 5);

    protected HeartBeatScheduler heartbeatChecker = new HeartBeatScheduler(()->{
        checkLeader();
    }, heartbeatTimeout.toMillis());

    protected void checkLeader() {
        //no-op. implemented by implementations.
    }


    public final void start() {
        peerListener.start();
        clientListener.start();
        onStart();
    }

    //subclasses can execute logic
    //at startup.
    // e.g. starting HeartBeat mechanism
    protected void onStart() {

    }

    //Send message without expecting any messages as a response from the peer
    //@see sendRequestToReplicas which expects a message from the peer.
    protected <T extends MessagePayload> void sendOneway(InetAddressAndPort address, T request, int correlationId) {
        try {
            network.sendOneWay(address, new RequestOrResponse(request.getMessageId().getId(), serialize(request), correlationId, getPeerConnectionAddress()));
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + address + " from " + getName());
        }
    }

    //Send message to peer and expect a separate message as response.
    //Once the message is received, the callback is invoked.
    //The response message types are configured to invoke responseMessageHandler which invokes the callback
    //@see responseMessageHandler
    public <T> void sendMessageToReplicas(RequestCallback callback, MessageId messageId, T requestToReplicas) {
        for (InetAddressAndPort replica : peerAddresses) {
            int correlationId = newCorrelationId();
            RequestOrResponse request = new RequestOrResponse(messageId.getId(), serialize(requestToReplicas), correlationId, getPeerConnectionAddress());
            sendMessageToReplica(callback, replica, request);
        }
    }

    //Sends message to replica and expects that the replica will send back a message with the same correlationId.
    //The message is kept waiting in the RequestWaitingList and expired if the replica fails to send message back.
    public void sendMessageToReplica(RequestCallback callback, InetAddressAndPort replicaAddress, RequestOrResponse request) {
        try {
            logger.debug(getName() + " Sending " + MessageId.valueOf(request.getRequestId()) + " to " + replicaAddress + " with CorrelationId:" + request.getCorrelationId());
            requestWaitingList.add(request.getCorrelationId(), callback);
            network.sendOneWay(replicaAddress, request);
         } catch (IOException e) {
            logger.error("Communication failure sending request to " + replicaAddress + " from " + getName());
            //If communication fails, it should immidiately report it to the callback.
            //Otherwise if a quorum of replica could not be reached, the callback will never complete.
            requestWaitingList.handleError(request.getCorrelationId(), e);
         }
    }

    public <T extends MessagePayload> void sendOnewayMessageToReplicas(T requestToReplicas) {
        for (InetAddressAndPort replica : peerAddresses) {
            int correlationId = newCorrelationId();
            sendOneway(replica, requestToReplicas, correlationId);
        }
    }

    public <T extends MessagePayload> void sendOnewayMessageToOtherReplicas(T requestToReplicas) {
        for (InetAddressAndPort replica : otherReplicas()) {
            int correlationId = newCorrelationId();
            sendOneway(replica, requestToReplicas, correlationId);
        }
    }

    private List<InetAddressAndPort> otherReplicas() {
        return peerAddresses.stream().filter(r -> !r.equals(peerConnectionAddress)).collect(Collectors.toList());
    }

    final Map<MessageId, MessageHandler> handlers = new HashMap<>();

    //handles messages sent by peers in the cluster in message passing style.
    //peer to peer communication happens on peerConnectionAddress
    public void handlePeerMessage(Message<RequestOrResponse> message)
    {
        var messageHandler = handlers.get(message.getMessageId());
        var deserializedRequest = deserialize(message.messagePayload(), messageHandler.requestClass);
        singularUpdateQueueExecutor.submit(()->{
            markHeartbeatReceived(); //TODO: Mark heartbeats in message handlings explcitily. As this can be user request as well.
            RequestOrResponse request = message.messagePayload();
            MessageId key = MessageId.valueOf(request.getRequestId());
            messageHandler.handler.apply(new Message<>(deserializedRequest, message.header));
        });
    }

    protected void markHeartbeatReceived() {
        heartbeatReceivedNs = clock.nanoTime();
    }

    //handles requests sent by clients of the cluster.
    //rpc requests are sent by clients on the clientConnectionAddress
    public void handleClientRequest(Message<RequestOrResponse> message) {
        var messageHandler = handlers.get(message.getMessageId());
        var deserializedRequest = deserialize(message.messagePayload(), messageHandler.requestClass);
        singularUpdateQueueExecutor.submit(() -> {
            RequestOrResponse request = message.messagePayload();
            Function<Object, CompletableFuture<?>> handler = messageHandler.handler;
            handler.apply(deserializedRequest)
                    .whenComplete((response , throwable)-> {
                        respondToClient(response, throwable, message.getCorrelationId(), message.getClientConnection(), request.getRequestId());
                    });
        });
        ;

    }

    private static void respondToClient(Object response, Throwable throwable, int correlationId, ClientConnection clientConnection, Integer requestId) {
        if (throwable != null) {
            clientConnection.write(new RequestOrResponse(requestId, serialize(throwable.getMessage()), correlationId).setError());
        } else {
            clientConnection.write(new RequestOrResponse(requestId, serialize(response), correlationId));
        }
    }

    public void addClockSkew(Duration duration) {
        clock.addClockSkew(duration);
    }

    //Configures a handler to process a message.
    //Sends the response from the handler
    // as a separate message to the sender.

    /**
     * One way message passing communication.
     * But the receiver of the message is supposed to send message to sender
     * The message handler returns a response which is sent as a message to the sender.
     * @see RequestWaitingList comes in handy here, as the response message is expected
     * by the sender and passed to the RequestWaitingList to handle.
     * @see replicate.paxos.SingleValuePaxos as an example.
     *
     * +----------+                +----------------+             +-----------+
     * |          |                |Request         |             |           |
     * |node1     |                |WaitingList     |             | node2     |
     * |          |                |                |             |           |
     * +----+-----+                +--------+-------+             +-----+-----+
     *      |                               |                           |
     *      | add(correlationId, callback)  |                           |
     *      +------------------------------>+                           |
     *      |                               |                           |
     *      |   message(correlationId)      |                           |
     *      +----------------------------------------------------------->
     *      |                               |                           |
     *      |                               |                           |
     *      |  message(correlationId)       |                           |
     *      <-----------------------------------------------------------+
     *      |                               |                           |
     *      |   handleResponse(message)     |                           |
     *      +------------------------------>+                           |
     *      |                               |                           |     *
     * */

    /**
     * One way message passing communication. The message handler does not return a response.
     * But is expected to send a messages as part of its handling the message. The message
     * is not necessarily sent to the sender but might be broadcasted.
     * No RequestWaitingList as such is needed here. But each peer needs to track the state
     * needed for handling and responding to the messages.
     * @see replicate.vsr.ViewStampedReplication
     *
     * */

    static class MessageHandler<Req extends MessagePayload, Res> {
        Class requestClass;
        Function<Message<Req>, Res> handler;

        public MessageHandler(Class requestClass, Function<Message<Req>, Res> handler) {
            this.requestClass = requestClass;
            this.handler = handler;
        }
    }



    public <Req extends MessagePayload> void handlesMessage(MessageId messageId, Consumer<Message<Req>> handler, Class<Req> requestClass) {
       Function<Message<Req>, Void> functionWrapper = reqMessage -> {
           handler.accept(reqMessage);
           return null;
       };
        handlers.put(messageId, new MessageHandler(requestClass, functionWrapper));
    }

    //Configures a handler to process a given request.
    //Sends response from the handler to the sender.
    //This is request-response  communication or rpc.
    //The sender expects a response to the request on the same connection.
    public <T  extends MessagePayload, Res> Replica handlesRequestAsync(MessageId messageId, Function<T, CompletableFuture<Res>> handler, Class<T> requestClass) {
        handlers.put(messageId, new MessageHandler(requestClass, handler));
        return this;
    }

    protected <T> void handleResponse(Message<T> message) {
        requestWaitingList.handleResponse(message.getCorrelationId(), message.messagePayload(), message.getFromAddress());
    }

    public int getServerId() {
        return config.getServerId();
    }
    private int newCorrelationId() {
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

    protected abstract void registerHandlers();

    public void shutdown() {
        peerListener.shudown();
        clientListener.shudown();
        heartbeatChecker.stop();
        heartBeatScheduler.stop();
        network.closeAllConnections();
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
