package distrib.patterns.common;

public interface Replica {
    void handleClientRequest(Message<RequestOrResponse> message);

    void handleServerMessage(Message<RequestOrResponse> message);

    void setNode(Node node);
}
