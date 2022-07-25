package paxos;

import net.requestwaitinglist.MonotonicId;

public class ProposalRequest {
    private MonotonicId monotonicId;
    private String proposedValue;

    public ProposalRequest(MonotonicId monotonicId, String proposedValue) {
        this.monotonicId = monotonicId;
        this.proposedValue = proposedValue;
    }

    public MonotonicId getMonotonicId() {
        return monotonicId;
    }

    public String getProposedValue() {
        return proposedValue;
    }
    //for jackson
    private ProposalRequest() {

    }
}
