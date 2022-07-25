package net.pipeline;

import common.JsonSerDes;
import common.RequestOrResponse;
import net.InetAddressAndPort;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Client {
    final PipelinedConnection socketChannel;
    private InetAddressAndPort to;
    private int correlationId;
    public Client(InetAddressAndPort to, Consumer<RequestOrResponse> responseConsumer) {
        this.to = to;
        socketChannel = new PipelinedConnection(to , TimeUnit.SECONDS.toMillis(1), responseConsumer);
        socketChannel.start();
    }

    public void send(String message) {
        socketChannel.send(newHeartbeatRequest(1));
    }

    private RequestOrResponse newHeartbeatRequest(Integer serverId) {
        return new RequestOrResponse(1, serialize(new HeartbeatRequest(serverId)), correlationId++);
    }

    private byte[] serialize(HeartbeatRequest heartbeatRequest) {
        return JsonSerDes.serialize(heartbeatRequest);
    }

}
