package replicate.mpaxoswithheartbeats;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class HeartbeatResponse extends Request {
    public final boolean success;
    public final MonotonicId fullLogBallot;

    public HeartbeatResponse(boolean success, MonotonicId fullLogBallot) {
        super(RequestId.HeartBeatResponse);
        this.success = success;
        this.fullLogBallot = fullLogBallot;
    }
}
