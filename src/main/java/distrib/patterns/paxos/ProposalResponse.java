package distrib.patterns.paxos;

public class ProposalResponse {
    boolean success;

    public ProposalResponse(boolean success) {
        this.success = success;
    }

    private ProposalResponse() {

    }
}
