package replicate.mpaxoswithheartbeats;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class HeartbeatRequest extends MessagePayload {
    public final MonotonicId ballot;

    public HeartbeatRequest(MonotonicId ballot) {
        super(MessageId.HeartBeatRequest);
        this.ballot = ballot;
    }
}
