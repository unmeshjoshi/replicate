package replicate.paxoskv.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class PrepareRequest extends Request {
    public final String key;
    public final MonotonicId generation;

    public PrepareRequest(String key, MonotonicId generation) {
        super(RequestId.Prepare);
        this.key = key;
        this.generation = generation;
    }
}
