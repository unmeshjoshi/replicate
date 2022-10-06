package distrib.patterns.paxos.messages;

import distrib.patterns.common.MonotonicId;
import distrib.patterns.common.Request;
import distrib.patterns.common.RequestId;

public class CommitRequest extends Request {
    private MonotonicId id;
    private String value;
    public CommitRequest(MonotonicId id, String value) {
        this();
        this.id = id;
        this.value = value;
    }

    public MonotonicId getId() {
        return id;
    }

    public String getValue() {
        return value;
    }

    private CommitRequest() {
        super(RequestId.Commit);
    }
}
