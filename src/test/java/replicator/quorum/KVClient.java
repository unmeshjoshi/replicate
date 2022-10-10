package replicator.quorum;

import distrib.patterns.common.*;
import replicator.net.InetAddressAndPort;
import replicator.net.SocketClient;
import replicator.quorum.messages.GetValueRequest;
import replicator.quorum.messages.SetValueRequest;
import replicator.quorum.messages.SetValueResponse;
import replicator.common.JsonSerDes;
import replicator.common.RequestId;
import replicator.common.RequestOrResponse;

import java.io.IOException;
import java.util.Random;

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
        StoredValue response = JsonSerDes.deserialize(getResponse.getMessageBodyJson(), StoredValue.class);
        return response.getValue();
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
