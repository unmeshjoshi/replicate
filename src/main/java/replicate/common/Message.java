package replicate.common;


import replicate.net.ClientConnection;
import replicate.net.InetAddressAndPort;


public class Message<T> {
    private final T payload;
    private final ClientConnection clientSocket;
    final Header header;

    public Message(T payload, Header header) {
        this(payload, header, null);
    }

    public Message(T payload,  Header header, ClientConnection clientSocket) {
        this.header = header;
        this.payload = payload;
        this.clientSocket = clientSocket;
    }

    public T messagePayload() {
        return payload;
    }

    public ClientConnection getClientConnection() {
        return clientSocket;
    }

    public MessageId getMessageId() {
        return header.messageId;
    }

    public int getCorrelationId() {
        return header.correlationId;
    }

    public InetAddressAndPort getFromAddress() {
        return header.fromAddress;
    }

    public record Header(InetAddressAndPort fromAddress, int correlationId, MessageId messageId){};

}
