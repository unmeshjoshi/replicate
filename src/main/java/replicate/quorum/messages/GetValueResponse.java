package replicate.quorum.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;
import replicate.quorum.StoredValue;

public class GetValueResponse extends MessagePayload {
    public final StoredValue value;

    public GetValueResponse(StoredValue value) {
        super(MessageId.GetValueResponse);
        this.value = value;
    }
}
