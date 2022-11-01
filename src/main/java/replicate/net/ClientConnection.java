package replicate.net;


import replicate.common.RequestOrResponse;

public interface ClientConnection {
    void write(RequestOrResponse response);
    void close();
}
