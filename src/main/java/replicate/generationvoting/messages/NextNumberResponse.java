package replicate.generationvoting.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class NextNumberResponse extends MessagePayload {
    int number;

    public NextNumberResponse(int number) {
        super(MessageId.NextNumberResponse);
        this.number = number;

    }

    public int getNumber() {
        return number;
    }

    //for jackson
    private NextNumberResponse() {
        super(MessageId.NextNumberResponse);
    }
}
