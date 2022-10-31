package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class PrepareRequest extends MessagePayload {
    public final MonotonicId monotonicId;

    public PrepareRequest(MonotonicId monotonicId) {
        super(MessageId.Prepare);
        this.monotonicId = monotonicId;
    }
}
