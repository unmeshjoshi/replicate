package replicate.quorum.messages;

import replicate.common.Request;
import replicate.common.RequestId;

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
