package distrib.patterns.net;


import distrib.patterns.common.RequestOrResponse;

public interface ClientConnection {
    void write(RequestOrResponse response);
    void close();
}
