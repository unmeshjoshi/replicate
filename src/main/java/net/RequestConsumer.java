package net;

import common.Message;
import common.RequestOrResponse;

public interface RequestConsumer {
    default void close(ClientConnection connection) {}
    void accept(Message<RequestOrResponse> request);
}
