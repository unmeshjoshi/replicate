package replicator.paxos.messages;

import replicator.common.MonotonicId;
import replicator.common.Request;
import replicator.common.RequestId;

public class CommitRequest extends Request {
    private MonotonicId generation;
    private String value;
    public CommitRequest(MonotonicId generation, String value) {
        this();
        this.generation = generation;
        this.value = value;
    }

    public MonotonicId getGeneration() {
        return generation;
    }

    public String getValue() {
        return value;
    }

    private CommitRequest() {
        super(RequestId.Commit);
    }
}
