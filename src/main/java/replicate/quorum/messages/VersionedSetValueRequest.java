package replicate.quorum.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class VersionedSetValueRequest extends MessagePayload {
    private long clientId;
    private int requestNumber;
    private String key;
    private String value;
    private long version;

    //for jaxon
    private VersionedSetValueRequest() {
        super(MessageId.VersionedSetValueRequest);
    }

    public VersionedSetValueRequest(String key, String value, long clientId, int requestNumber, long version) {
        super(MessageId.VersionedSetValueRequest);
        this.key = key;
        this.value = value;
        this.clientId = clientId;
        this.requestNumber = requestNumber;
        this.version = version;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getClientId() {
        return clientId;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public long getTimestamp() {
        return version;
    }
}


