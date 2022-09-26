package distrib.patterns.quorum.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class GetValueRequest extends Request {
    private String key;
    public GetValueRequest(String key) {
        super(RequestId.GetValueRequest);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    //for jackson
    private GetValueRequest() {
        super(RequestId.GetValueRequest);
    }
}
