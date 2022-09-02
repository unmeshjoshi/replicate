package distrib.patterns.quorum;

import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class Network {
    List<InetAddressAndPort> dropRequestsTo = new ArrayList<>();
    Map<InetAddressAndPort, Integer> noOfMessages = new HashMap<>();
    Map<InetAddressAndPort, Integer> dropAfter = new HashMap<>();
    public void sendOneWay(InetAddressAndPort address, RequestOrResponse message) throws IOException {
        if (dropRequestsTo.contains(address) || noOfMessagesReachedLimit(address)) {
            return;
        }

        sendMessage(address, message);
    }

    private void sendMessage(InetAddressAndPort address, RequestOrResponse message) throws IOException {
        SocketClient socketClient = new SocketClient(address);
        socketClient.sendOneway(message);
        Integer integer = noOfMessages.get(address);
        if (integer == null) {
            integer = 0;
        }
        noOfMessages.put(address, integer + 1);
    }

    private boolean noOfMessagesReachedLimit(InetAddressAndPort address) {
        Integer dropAfterMessages = dropAfter.get(address);
        Integer noOfMessages = this.noOfMessages.get(address);
        return dropAfterMessages == null?false:(noOfMessages != null && noOfMessages == dropAfterMessages);
    }

    public void dropMessagesTo(InetAddressAndPort address) {
        dropRequestsTo.add(address);
    }

    public void reconnectTo(InetAddressAndPort address) {
        dropRequestsTo.remove(address);
    }

    public void dropMessagesAfter(InetAddressAndPort address, int dropAfterNoOfMessages) {
        dropAfter.put(address, dropAfterNoOfMessages);
    }

    List<InetAddressAndPort> delayMessagesTo = new ArrayList<>();
    public void addDelayForMessagesTo(InetAddressAndPort peerConnectionAddress) {
        delayMessagesTo.add(peerConnectionAddress);
    }
}
