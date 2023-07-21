package replicate.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.net.InetAddressAndPort;
import replicate.net.SocketClient;
import replicate.quorum.QuorumKVStore;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class Network {
    private static Logger logger = LogManager.getLogger(Network.class);

    public static final int MESSAGE_DELAY = 1000;

    List<InetAddressAndPort> dropRequestsTo = new ArrayList<>();
    Map<InetAddressAndPort, Integer> noOfMessages = new HashMap<>();
    Map<InetAddressAndPort, Integer> dropAfter = new HashMap<>();
    Map<InetAddressAndPort, Integer> delayMessagesAfter = new HashMap<>();
    Map<InetAddressAndPort, Set<MessageId>> delayMessageTypes =
            new HashMap<>();

    ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    public void sendOneWay(InetAddressAndPort address, RequestOrResponse message) throws IOException {
        if (dropRequestsTo.contains(address) || noOfMessagesReachedLimit(address)) {
            removeExistingConnections(address);
            throw new IOException("Unable to connect to " + address);
        }

        if (shouldDelayMessagesOfType(address, MessageId.valueOf(message.getRequestId()))) {
            sendAfterDelay(address, message, MESSAGE_DELAY);
            return;
        }

        if (shouldDelayMessagesTo(address)) {
            sendAfterDelay(address, message, MESSAGE_DELAY);
            return;
        }
        logger.info("Sending " + MessageId.valueOf(message.getRequestId()) +
                " to " + address);
        sendMessage(address, message);
    }

    private boolean shouldDelayMessagesOfType(InetAddressAndPort address, MessageId messageId) {
        Set<MessageId> ids = delayMessageTypes.getOrDefault(address,
                new HashSet<>());
        return ids.contains(messageId);
    }

    private void removeExistingConnections(InetAddressAndPort address) {
        SocketClient socketClient = connectionPool.remove(address);
        if (socketClient != null) {
            socketClient.close();
        }
    }

    private void sendAfterDelay(InetAddressAndPort address,
                                RequestOrResponse message,
                                long delay) {
        executor.schedule(()->{
            try {
                logger.info("Sending delayed message "
                                + MessageId.valueOf(message.getRequestId())
                        + " to address = " +
                                address);
                sendMessage(address, message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private boolean shouldDelayMessagesTo(InetAddressAndPort address) {
        Integer delayAfterNRequests = delayMessagesAfter.get(address);
        Integer noOfRequestsSent = noOfMessages.get(address);
        if ((delayAfterNRequests == null) || (noOfRequestsSent == null)) {
            return false;
        }
        return noOfRequestsSent >= delayAfterNRequests;
    }

    Map<InetAddressAndPort, SocketClient> connectionPool = new HashMap<>();

    private void sendMessage(InetAddressAndPort address, RequestOrResponse message) throws IOException {
        SocketClient socketClient = getOrCreateConnection(address);
        socketClient.sendOneway(message);
        Integer integer = noOfMessages.get(address);
        if (integer == null) {
            integer = 0;
        }
        noOfMessages.put(address, integer + 1);
    }

    private RequestOrResponse sendAndReceive(InetAddressAndPort address, RequestOrResponse message) throws IOException {
        SocketClient socketClient = getOrCreateConnection(address);
        RequestOrResponse response = socketClient.blockingSend(message);
        Integer integer = noOfMessages.get(address);
        if (integer == null) {
            integer = 0;
        }
        noOfMessages.put(address, integer + 1);
        return response;
    }

    private synchronized SocketClient getOrCreateConnection(InetAddressAndPort address) throws IOException {
        SocketClient socketClient = connectionPool.get(address);
        if (socketClient == null || !socketClient.isClosed()) {
            socketClient = new SocketClient(address);
            connectionPool.put(address, socketClient);
        }
        return socketClient;
    }

    private boolean noOfMessagesReachedLimit(InetAddressAndPort address) {
        Integer dropAfterMessages = dropAfter.get(address);
        Integer noOfMessages = this.noOfMessages.get(address);
        return dropAfterMessages == null?false:(noOfMessages != null && noOfMessages >= dropAfterMessages);
    }

    public void dropMessagesTo(InetAddressAndPort address) {
        dropRequestsTo.add(address);
    }

    public void reconnectTo(InetAddressAndPort address) {
        dropRequestsTo.remove(address);
        dropAfter.remove(address);
        delayMessagesAfter.remove(address);
        noOfMessages.remove(address); //also reset message counter to specific address.
    }

    public void dropMessagesAfter(InetAddressAndPort address, int dropAfterNoOfMessages) {
        noOfMessages.remove(address); //only count messages here after.
        dropAfter.put(address, dropAfterNoOfMessages);
    }

    public void addDelayForMessagesToAfterNMessages(InetAddressAndPort peerConnectionAddress, int noOfMessages) {
        delayMessagesAfter.put(peerConnectionAddress, noOfMessages);
    }

    public void addDelayForMessagesOfType(InetAddressAndPort peerConnectionAddress,
                                          MessageId messageId) {
        Set<MessageId> messageIds = delayMessageTypes.get(peerConnectionAddress);
        if (messageIds == null) {
            messageIds = new HashSet<>();
        }
        messageIds.add(messageId);
        delayMessageTypes.put(peerConnectionAddress, messageIds);
    }

    public void closeAllConnections() {
        Set<InetAddressAndPort> inetAddressAndPorts = connectionPool.keySet();
        for (InetAddressAndPort inetAddressAndPort : inetAddressAndPorts) {
            connectionPool.get(inetAddressAndPort).close();
        }
        connectionPool.clear();
    }
}
