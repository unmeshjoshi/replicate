package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class PrepareRequest extends MessagePayload {
    public final Integer index;
    public final MonotonicId monotonicId;

    public PrepareRequest(Integer index, MonotonicId monotonicId) {
        super(MessageId.Prepare);
        this.index = index;
        this.monotonicId = monotonicId;
    }
}
