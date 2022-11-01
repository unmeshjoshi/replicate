package replicate.quorumconsensus.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class GetVersionResponse extends MessagePayload {
    MonotonicId id;

    public GetVersionResponse(MonotonicId id) {
        super(MessageId.GetVersionResponse);
        this.id = id;
    }

    //for jackson
    private GetVersionResponse() {
        super(MessageId.GetVersionResponse);
    }

    public MonotonicId getVersion() {
        return id;
    }
}
