package replicator.quorumconsensus.messages;

import replicator.common.Request;
import replicator.common.RequestId;

public class SetValueRequest extends Request {
    public final String key;
    public final String value;

    public SetValueRequest(String key, String value) {
        super(RequestId.SetValueRequest);
        this.key = key;
        this.value = value;
    }
}


