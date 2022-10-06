package distrib.patterns.paxoskv.messages;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class PrepareRequest extends Request {
    public final String key;
    public final MonotonicId generation;

    public PrepareRequest(String key, MonotonicId generation) {
        super(RequestId.Prepare);
        this.key = key;
        this.generation = generation;
    }
}
