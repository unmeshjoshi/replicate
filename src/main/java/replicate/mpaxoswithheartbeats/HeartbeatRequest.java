package replicate.mpaxoswithheartbeats;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class HeartbeatRequest extends Request {
    public final MonotonicId ballot;

    public HeartbeatRequest(MonotonicId ballot) {
        super(RequestId.HeartBeatRequest);
        this.ballot = ballot;
    }
}
