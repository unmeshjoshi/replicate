package distrib.patterns.paxoskv;

import distrib.patterns.common.MonotonicId;

public class CommitRequest {
    private String key;
    private String value;
    private MonotonicId monotonicId;

    public CommitRequest(String key, String value, MonotonicId monotonicId) {
        this.key = key;
        this.value = value;
        this.monotonicId = monotonicId;
    }

    public String getKey() {
        return key;
    }

    public MonotonicId getMonotonicId() {
        return monotonicId;
    }

    public String getValue() {
        return value;
    }
}
