package replicate.paxoslog.messages;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

import java.time.Duration;

public class PrepareRequest extends MessagePayload {
    public final Integer index;
    public final MonotonicId monotonicId;
    public Duration leaderLeaseDuration;
    public PrepareRequest(Integer index, MonotonicId monotonicId) {
        super(MessageId.Prepare);
        this.index = index;
        this.monotonicId = monotonicId;
    }
}
