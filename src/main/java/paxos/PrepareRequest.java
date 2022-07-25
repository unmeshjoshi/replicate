package paxos;

import net.requestwaitinglist.MonotonicId;

public class PrepareRequest {
    public final MonotonicId monotonicId;

    public PrepareRequest(MonotonicId monotonicId) {
        this.monotonicId = monotonicId;
    }

    //for jackson
    private PrepareRequest() {
        monotonicId = MonotonicId.empty();
    }
}
