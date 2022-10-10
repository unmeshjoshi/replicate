package replicate.quorumconsensus.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class GetVersionRequest extends Request {
    String key;

    public GetVersionRequest(String key) {
        super(RequestId.GetVersion);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    //
    private GetVersionRequest() {
        super(RequestId.GetVersion);
    }
}
