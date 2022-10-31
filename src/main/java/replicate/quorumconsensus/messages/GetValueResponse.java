package replicate.quorumconsensus.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;
import replicate.quorumconsensus.StoredValue;

public class GetValueResponse extends MessagePayload {
    StoredValue value;

    public GetValueResponse(StoredValue value) {
        this();
        this.value = value;
    }

    private GetValueResponse() {
        super(MessageId.GetValueResponse);
    }

    public StoredValue getValue() {
        return value;
    }
}
