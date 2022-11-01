package replicate.quorumconsensus.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class SetValueRequest extends MessagePayload {
    public final String key;
    public final String value;

    public SetValueRequest(String key, String value) {
        super(MessageId.SetValueRequest);
        this.key = key;
        this.value = value;
    }
}


