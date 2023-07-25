package replicate.quorum;

import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.quorum.messages.GetValueRequest;
import replicate.quorum.messages.GetValueResponse;
import replicate.quorum.messages.SetValueRequest;
import replicate.quorum.messages.SetValueResponse;

import java.io.IOException;

public class KVClient {
    public NetworkClient.Response<GetValueResponse> getValue(InetAddressAndPort address, String key) throws IOException {
        NetworkClient client = new NetworkClient();
        NetworkClient.Response<GetValueResponse> response = client.sendAndReceive(new GetValueRequest(key), address,
                GetValueResponse.class);
        return response;
    }

    public NetworkClient.Response<SetValueResponse> setValue(InetAddressAndPort primaryNodeAddress, String key, String value) throws IOException {
        NetworkClient client = new NetworkClient();
        NetworkClient.Response<SetValueResponse> response =
                client.sendAndReceive(new SetValueRequest(key, value), primaryNodeAddress,
                SetValueResponse.class);
        return response;
    }
}
