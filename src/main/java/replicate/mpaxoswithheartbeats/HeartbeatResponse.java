package replicate.mpaxoswithheartbeats;

import replicate.common.MonotonicId;
import replicate.common.MessagePayload;
import replicate.common.MessageId;

public class HeartbeatResponse extends MessagePayload {
    public final boolean success;
    public final MonotonicId fullLogBallot;

    public HeartbeatResponse(boolean success, MonotonicId fullLogBallot) {
        super(MessageId.HeartBeatResponse);
        this.success = success;
        this.fullLogBallot = fullLogBallot;
    }
}
