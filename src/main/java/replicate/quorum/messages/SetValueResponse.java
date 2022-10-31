package replicate.quorum.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class SetValueResponse extends MessagePayload {
    public final String result;

    public SetValueResponse(String result) {
        super(MessageId.SetValueResponse);
        this.result = result;
    }
}
