package replicator.common;

import replicator.net.InetAddressAndPort;
import replicator.net.SocketClient;

import java.io.IOException;

public class NetworkClient {
    public <Req extends Request, Res> Res sendAndReceive(Req request, InetAddressAndPort address, Class<Res> responseClass) throws IOException {
        try(SocketClient<Object> client = new SocketClient<>(address)){
            RequestOrResponse getResponse = client.blockingSend(new RequestOrResponse(request.getRequestId().getId(),
                    JsonSerDes.serialize(request)));
            if (getResponse.isError()) {
                throw new RuntimeException(JsonSerDes.deserialize(getResponse.getMessageBodyJson(), String.class));
            };
            Res response = JsonSerDes.deserialize(getResponse.getMessageBodyJson(), responseClass);
            return response;
        }
    }
}
