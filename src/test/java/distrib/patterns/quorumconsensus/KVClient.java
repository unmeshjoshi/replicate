package distrib.patterns.quorumconsensus;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.Replica;
import distrib.patterns.common.RequestId;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;

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
        QuorumKV.SetValueResponse response = JsonSerDes.deserialize(setResponse.getMessageBodyJson(), QuorumKV.SetValueResponse.class);
        return response.getResult();
    }

    private RequestOrResponse createGetValueRequest(String key) {
        GetValueRequest getValueRequest = new GetValueRequest(key);
        RequestOrResponse requestOrResponse1 = new RequestOrResponse(RequestId.GetValueRequest.getId(), JsonSerDes.serialize(getValueRequest), correlationId++);
        return requestOrResponse1;
    }

    private RequestOrResponse createSetValueRequest(String key, String value) {
        SetValueRequest setValueRequest = new SetValueRequest(key, value);
        RequestOrResponse requestOrResponse = new RequestOrResponse(RequestId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), correlationId++);
        return requestOrResponse;
    }
}
