package replicate.chain.messages;

import replicate.common.MessageId;
import replicate.common.MessagePayload;

import java.util.UUID;

public class ChainWriteRequest extends MessagePayload {
    private final String key;
    private final String value;
    private final int version;
    private final UUID requestId;

    public ChainWriteRequest(String key, String value, UUID requestId) {
        super(MessageId.ChainWrite);
        this.key = key;
        this.value = value;
        this.version = 0;
        this.requestId = requestId;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    public UUID getRequestId() {
        return requestId;
    }
} 