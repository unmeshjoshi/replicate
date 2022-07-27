package distrib.patterns.net;

import distrib.patterns.common.Message;
import distrib.patterns.common.RequestOrResponse;

public interface RequestConsumer {
    default void close(ClientConnection connection) {}
    void accept(Message<RequestOrResponse> request);
}
