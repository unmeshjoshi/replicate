package replicate.quorumconsensus.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class VersionedSetValueRequest extends Request {
    public final String key;
    public final String value;
    public final MonotonicId version;

    public VersionedSetValueRequest(String key, String value, MonotonicId version) {
        super(RequestId.VersionedSetValueRequest);
        this.key = key;
        this.value = value;
        this.version = version;
    }
}


