package replicate.paxoskv.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class CommitRequest extends Request {
    public final String key;
    public final byte[] value;
    public final MonotonicId generation;

    public CommitRequest(String key, byte[] value, MonotonicId generation) {
        super(RequestId.Commit);
        this.key = key;
        this.value = value;
        this.generation = generation;
    }
}
