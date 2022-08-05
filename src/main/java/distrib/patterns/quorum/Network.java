package distrib.patterns.quorum;

import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class Network {
    List<InetAddressAndPort> dropRequestsTo = new ArrayList<>();

    public void sendOneWay(InetAddressAndPort address, RequestOrResponse message) throws IOException {
        if (dropRequestsTo.contains(address)) {
            return;
        }
        SocketClient socketClient = new SocketClient(address);
        socketClient.sendOneway(message);
    }

    public void dropMessagesTo(InetAddressAndPort address) {
        dropRequestsTo.add(address);
    }

    public void reconnectTo(InetAddressAndPort address) {
        dropRequestsTo.remove(address);
    }
}
