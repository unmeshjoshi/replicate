package replicate.generationvoting.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class NextNumberResponse extends MessagePayload {
    public final int number;

    public NextNumberResponse(int number) {
        super(MessageId.NextNumberResponse);
        this.number = number;

    }
}
