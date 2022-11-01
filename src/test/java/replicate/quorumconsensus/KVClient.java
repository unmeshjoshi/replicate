package replicate.quorumconsensus;

import replicate.common.JsonSerDes;
import replicate.common.MessageId;
import replicate.common.RequestOrResponse;
import replicate.net.InetAddressAndPort;
import replicate.net.SocketClient;
import replicate.quorumconsensus.messages.GetValueRequest;
import replicate.quorumconsensus.messages.SetValueRequest;
import replicate.quorumconsensus.messages.SetValueResponse;

import java.io.IOException;

public class KVClient {
    int correlationId;
    public String getValue(InetAddressAndPort cyreneAddress, String key) throws IOException {
        RequestOrResponse requestOrResponse1 = createGetValueRequest(key);
        SocketClient<Object> client = new SocketClient<>(cyreneAddress);
        RequestOrResponse getResponse = client.blockingSend(requestOrResponse1);
        client.close();
        if (getResponse.isError()) {
            return "Error";
        };

        StoredValue storedValue = JsonSerDes.deserialize(getResponse.getMessageBodyJson(), StoredValue.class);
        return storedValue.getValue();
    }

    public String setValue(InetAddressAndPort primaryNodeAddress, String title, String microservices) throws IOException {
        SocketClient client = new SocketClient(primaryNodeAddress);
        RequestOrResponse requestOrResponse = createSetValueRequest(title, microservices);
        RequestOrResponse setResponse = client.blockingSend(requestOrResponse);
        client.close();
        if (setResponse.isError()) {
            return "Error";
        };
        SetValueResponse response = JsonSerDes.deserialize(setResponse.getMessageBodyJson(), SetValueResponse.class);
        return response.getResult();
    }

    private RequestOrResponse createGetValueRequest(String key) {
        GetValueRequest getValueRequest = new GetValueRequest(key);
        RequestOrResponse requestOrResponse1 = new RequestOrResponse(MessageId.GetValueRequest.getId(), JsonSerDes.serialize(getValueRequest), correlationId++);
        return requestOrResponse1;
    }

    private RequestOrResponse createSetValueRequest(String key, String value) {
        SetValueRequest setValueRequest = new SetValueRequest(key, value);
        RequestOrResponse requestOrResponse = new RequestOrResponse(MessageId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), correlationId++);
        return requestOrResponse;
    }
}
