package replicator.net;


import replicator.common.RequestOrResponse;

public interface ClientConnection {
    void write(RequestOrResponse response);
    void close();
}
