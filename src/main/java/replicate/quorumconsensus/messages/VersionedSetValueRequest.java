package replicate.quorumconsensus.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class VersionedSetValueRequest extends MessagePayload {
    public final String key;
    public final String value;
    public final MonotonicId version;

    public VersionedSetValueRequest(String key, String value, MonotonicId version) {
        super(MessageId.VersionedSetValueRequest);
        this.key = key;
        this.value = value;
        this.version = version;
    }
}


