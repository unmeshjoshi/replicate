package distrib.patterns.paxos.messages;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

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
