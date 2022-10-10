package replicator.quorumconsensus.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;

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


