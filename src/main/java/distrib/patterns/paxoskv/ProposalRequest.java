package distrib.patterns.paxoskv;

import distrib.patterns.paxos.MonotonicId;

public class ProposalRequest {
    private MonotonicId monotonicId;
    private String key;
    private String proposedValue;

    public ProposalRequest(MonotonicId monotonicId, String key, String proposedValue) {
        this.monotonicId = monotonicId;
        this.key = key;
        this.proposedValue = proposedValue;
    }

    public MonotonicId getMonotonicId() {
        return monotonicId;
    }

    public String getKey() {
        return key;
    }

    public String getProposedValue() {
        return proposedValue;
    }
    //for jackson
    private ProposalRequest() {

    }
}
