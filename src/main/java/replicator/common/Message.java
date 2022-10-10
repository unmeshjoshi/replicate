package replicator.common;


import replicator.net.ClientConnection;

public class Message<T> {
    private final T clientRequest;
    private final ClientConnection clientSocket;
    private RequestId requestId;
    private int groupId;

    public Message(T t, RequestId requestId) {
        this(t, requestId, null);
    }

    public Message(T t, RequestId requestId, ClientConnection clientSocket) {
        this(t, -1, requestId, clientSocket);
    }

    public Message(T t, int groupId, RequestId requestId, ClientConnection clientSocket) {
        this.clientRequest = t;
        this.groupId = groupId;
        this.requestId = requestId;
        this.clientSocket = clientSocket;
    }

    public Message(RequestId requestId) {
        this(null, requestId);
    }

    public T getRequest() {
        return clientRequest;
    }

    public ClientConnection getClientConnection() {
        return clientSocket;
    }

    public RequestId getRequestId() {
        return requestId;
    }

    public int getGroupId() {
        return groupId;
    }
}
