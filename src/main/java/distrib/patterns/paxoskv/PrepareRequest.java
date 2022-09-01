package distrib.patterns.paxoskv;

import distrib.patterns.common.MonotonicId;

public class PrepareRequest {
    public final String key;
    public final MonotonicId monotonicId;

    public PrepareRequest(String key, MonotonicId monotonicId) {
        this.key = key;
        this.monotonicId = monotonicId;
    }

    //for jackson
    private PrepareRequest() {
        monotonicId = MonotonicId.empty();
        key="";
    }
}
