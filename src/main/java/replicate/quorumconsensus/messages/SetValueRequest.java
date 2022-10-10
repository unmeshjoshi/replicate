package replicate.quorumconsensus.messages;

import replicate.common.Request;
import replicate.common.RequestId;

public class SetValueRequest extends Request {
    public final String key;
    public final String value;

    public SetValueRequest(String key, String value) {
        super(RequestId.SetValueRequest);
        this.key = key;
        this.value = value;
    }
}


