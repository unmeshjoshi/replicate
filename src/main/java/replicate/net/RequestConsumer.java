package replicate.net;

import replicate.common.Message;
import replicate.common.RequestOrResponse;

public interface RequestConsumer {
    default void close(ClientConnection connection) {}
    void accept(Message<RequestOrResponse> request);
}
