package replicator.net;

import replicator.common.Message;
import replicator.common.RequestOrResponse;

public interface RequestConsumer {
    default void close(ClientConnection connection) {}
    void accept(Message<RequestOrResponse> request);
}
