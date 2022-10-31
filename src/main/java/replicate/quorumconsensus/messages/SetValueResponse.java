package replicate.quorumconsensus.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class SetValueResponse extends MessagePayload {
    String result;

    public SetValueResponse(String result) {
        this();
        this.result = result;
    }

    private SetValueResponse() {
        super(MessageId.SetValueResponse);
    }

    public String getResult() {
        return result;
    }
}
