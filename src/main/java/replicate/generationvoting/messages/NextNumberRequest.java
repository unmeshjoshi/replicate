package replicate.generationvoting.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class NextNumberRequest extends MessagePayload {
    public NextNumberRequest() {
        super(MessageId.NextNumberRequest);
    }
}
