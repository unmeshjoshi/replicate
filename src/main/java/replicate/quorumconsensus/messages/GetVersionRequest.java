package replicate.quorumconsensus.messages;

import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class GetVersionRequest extends MessagePayload {
    String key;

    public GetVersionRequest(String key) {
        super(MessageId.GetVersion);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    //
    private GetVersionRequest() {
        super(MessageId.GetVersion);
    }
}
