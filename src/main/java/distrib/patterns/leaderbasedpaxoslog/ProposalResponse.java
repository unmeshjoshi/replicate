package distrib.patterns.leaderbasedpaxoslog;

public class ProposalResponse {
    boolean success;

    public ProposalResponse(boolean success) {
        this.success = success;
    }

    private ProposalResponse() {

    }
}
