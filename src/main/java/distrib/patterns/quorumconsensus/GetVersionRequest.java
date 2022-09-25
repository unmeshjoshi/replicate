package distrib.patterns.quorumconsensus;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

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
