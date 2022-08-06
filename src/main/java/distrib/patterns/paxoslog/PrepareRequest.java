package distrib.patterns.paxoslog;

import distrib.patterns.paxos.MonotonicId;

public class PrepareRequest {
    public final Integer index;
    public final MonotonicId monotonicId;

    public PrepareRequest(Integer index, MonotonicId monotonicId) {
        this.index = index;
        this.monotonicId = monotonicId;
    }

    //for jackson
    private PrepareRequest() {
        monotonicId = MonotonicId.empty();
        index=0;
    }
}
