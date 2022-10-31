package replicate.quorum.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;
import replicate.quorum.StoredValue;

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
