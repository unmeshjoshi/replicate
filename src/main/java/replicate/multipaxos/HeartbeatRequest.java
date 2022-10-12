package replicate.multipaxos;

import replicate.common.Request;
import replicate.common.RequestId;

public class HeartbeatRequest extends Request {
    public HeartbeatRequest() {
        super(RequestId.HeartBeatRequest);
    }
}
