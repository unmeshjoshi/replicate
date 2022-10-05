package distrib.patterns.common;

import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;

import java.io.IOException;

public class NetworkClient {
    public <Req extends Request, Res> Res sendAndReceive(Req request, InetAddressAndPort address, Class<Res> responseClass) throws IOException {
        try(SocketClient<Object> client = new SocketClient<>(address)){
            RequestOrResponse getResponse = client.blockingSend(new RequestOrResponse(request.getRequestId().getId(),
                    JsonSerDes.serialize(request)));
            Res response = JsonSerDes.deserialize(getResponse.getMessageBodyJson(), responseClass);
            return response;
        }
    }
}
