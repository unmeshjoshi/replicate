package replicate.common;

import replicate.net.ClientConnection;
import replicate.net.InetAddressAndPort;

public class Message<T> {
    private final T payload;
    private final ClientConnection clientSocket;
    final Header header;
    private final long timestamp;

    public Message(T payload, Header header) {
        this(payload, header, null, 0);
    }

    public Message(T payload,  Header header, ClientConnection clientSocket) {
        this(payload, header, clientSocket, 0);
    }

    public Message(T payload, Header header, long timestamp) {
        this(payload, header, null, timestamp);
    }

    public Message(T payload,  Header header, ClientConnection clientSocket, long timestamp) {
        this.header = header;
        this.payload = payload;
        this.clientSocket = clientSocket;
        this.timestamp = timestamp;
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

    public long getTimestamp() {
        return timestamp;
    }

    public record Header(InetAddressAndPort fromAddress, int correlationId, MessageId messageId){};

}
