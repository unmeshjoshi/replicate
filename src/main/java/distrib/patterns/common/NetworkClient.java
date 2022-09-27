package distrib.patterns.common;

import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;

import java.io.IOException;

public class NetworkClient<T> {
    Class<T> responseClass;

    public NetworkClient(Class<T> responseClass) {
        this.responseClass = responseClass;
    }

    public T send(Request request, InetAddressAndPort address) throws IOException {
        SocketClient<Object> client = new SocketClient<>(address);
        RequestOrResponse getResponse = client.blockingSend(new RequestOrResponse(request.getRequestId().getId(),
                JsonSerDes.serialize(request)));
        T response = JsonSerDes.deserialize(getResponse.getMessageBodyJson(), responseClass);
        return response;
    }
}
