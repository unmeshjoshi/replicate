package replicate.chain.messages;

import replicate.common.MessageId;
import replicate.common.MessagePayload;

public class ExecuteCommandRequest extends MessagePayload {
    private final String key;
    private final String value;

    public ExecuteCommandRequest(String key, String value) {
        super(MessageId.ExcuteCommandRequest);
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
} 