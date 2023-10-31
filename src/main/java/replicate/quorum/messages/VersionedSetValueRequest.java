package replicate.quorum.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class VersionedSetValueRequest extends MessagePayload {
    public final long clientId;
    public final int requestNumber;
    public final String key;
    public final String value;
    public final long version;

    public VersionedSetValueRequest(String key, String value, long clientId, int requestNumber, long version) {
        super(MessageId.VersionedSetValueRequest);
        this.key = key;
        this.value = value;
        this.clientId = clientId;
        this.requestNumber = requestNumber;
        this.version = version;
    }
}


