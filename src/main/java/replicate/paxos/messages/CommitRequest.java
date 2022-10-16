package replicate.paxos.messages;

import replicate.common.MonotonicId;
import replicate.common.Request;
import replicate.common.RequestId;

public class CommitRequest extends Request {
    private MonotonicId generation;
    private byte[] value;
    public CommitRequest(MonotonicId generation, byte[] value) {
        this();
        this.generation = generation;
        this.value = value;
    }

    public MonotonicId getGeneration() {
        return generation;
    }

    public byte[] getValue() {
        return value;
    }

    private CommitRequest() {
        super(RequestId.Commit);
    }
}
