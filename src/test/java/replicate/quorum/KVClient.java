package replicate.quorum;

import replicate.common.JsonSerDes;
import replicate.common.RequestId;
import replicate.common.RequestOrResponse;
import replicate.net.InetAddressAndPort;
import replicate.net.SocketClient;
import replicate.quorum.messages.GetValueRequest;
import replicate.quorum.messages.SetValueRequest;
import replicate.quorum.messages.SetValueResponse;

import java.io.IOException;
import java.util.Random;

public class KVClient {
    int correlationId;
    public String getValue(InetAddressAndPort address, String key) throws IOException {
        RequestOrResponse requestOrResponse1 = createGetValueRequest(key);
        SocketClient<Object> client = new SocketClient<>(address);
        RequestOrResponse getResponse = client.blockingSend(requestOrResponse1);
        client.close();
        if (getResponse.isError()) {
            return "Error";
        };
        StoredValue response = JsonSerDes.deserialize(getResponse.getMessageBodyJson(), StoredValue.class);
        return response.getValue();
    }

    public String setValue(InetAddressAndPort primaryNodeAddress, String key, String value) throws IOException {
        SocketClient client = new SocketClient(primaryNodeAddress);
        RequestOrResponse requestOrResponse = createSetValueRequest(key, value);
        RequestOrResponse setResponse = client.blockingSend(requestOrResponse);
        client.close();

        if (setResponse.isError()) {
            return "Error";
        };
        SetValueResponse response = JsonSerDes.deserialize(setResponse.getMessageBodyJson(), SetValueResponse.class);
        return response.result;
    }

    private RequestOrResponse createGetValueRequest(String key) {
        GetValueRequest getValueRequest = new GetValueRequest(key);
        RequestOrResponse requestOrResponse1 = new RequestOrResponse(RequestId.GetValueRequest.getId(), JsonSerDes.serialize(getValueRequest), new Random().nextInt());
        return requestOrResponse1;
    }

    private RequestOrResponse createSetValueRequest(String key, String value) {
        SetValueRequest setValueRequest = new SetValueRequest(key, value);
        RequestOrResponse requestOrResponse = new RequestOrResponse(RequestId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), new Random().nextInt());
        return requestOrResponse;
    }
}
