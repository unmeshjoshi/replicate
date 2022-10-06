package distrib.patterns.quorumconsensus.messages;

import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class SetValueRequest extends Request {
    public final String key;
    public final String value;

    public SetValueRequest(String key, String value) {
        super(RequestId.SetValueRequest);
        this.key = key;
        this.value = value;
    }
}


