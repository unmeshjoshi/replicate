package replicate.common;


import replicate.net.ClientConnection;
import replicate.net.InetAddressAndPort;


public class Message<T> {
    private final T clientRequest;
    private final ClientConnection clientSocket;
    final Header header;

    public Message(T t, Header header) {
        this(t, header, null);
    }

    public Message(T t,  Header header, ClientConnection clientSocket) {
        this.header = header;
        this.clientRequest = t;
        this.clientSocket = clientSocket;
    }

    public T getRequest() {
        return clientRequest;
    }

    public ClientConnection getClientConnection() {
        return clientSocket;
    }

    public RequestId getRequestId() {
        return header.requestId;
    }

    public int getCorrelationId() {
        return header.correlationId;
    }

    public InetAddressAndPort getFromAddress() {
        return header.fromAddress;
    }

    public record Header(InetAddressAndPort fromAddress, int correlationId, RequestId requestId){};

}
