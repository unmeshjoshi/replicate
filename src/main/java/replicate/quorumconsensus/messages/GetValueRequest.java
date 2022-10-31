package replicate.quorumconsensus.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class GetValueRequest extends MessagePayload {
    private String key;
    public GetValueRequest(String key) {
        super(MessageId.GetValueRequest);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    //for jackson
    private GetValueRequest() {
        super(MessageId.GetValueRequest);
    }
}
