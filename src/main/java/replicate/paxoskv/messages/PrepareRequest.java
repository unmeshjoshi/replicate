package replicate.paxoskv.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class PrepareRequest extends MessagePayload {
    public final String key;
    public final MonotonicId generation;

    public PrepareRequest(String key, MonotonicId generation) {
        super(MessageId.Prepare);
        this.key = key;
        this.generation = generation;
    }
}
